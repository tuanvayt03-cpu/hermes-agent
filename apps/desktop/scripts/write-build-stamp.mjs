/**
 * Writes apps/desktop/build/install-stamp.json with the git ref the desktop
 * .exe should pin to at first-launch bootstrap time.  This file ships inside
 * the packaged app via electron-builder's extraResources entry and is read
 * by electron/main.ts to drive the install.ps1 stage bootstrap flow.
 *
 * Schema (subject to bump via STAMP_SCHEMA_VERSION):
 *   {
 *     "schemaVersion": 1,
 *     "commit":        "<40-char SHA>",
 *     "branch":        "<branch name>",
 *     "builtAt":       "<ISO 8601 UTC timestamp>",
 *     "dirty":         true|false,
 *     "source":        "ci" | "local" | "fallback"
 *   }
 *
 * Source preference order:
 *   1. CI env vars ($GITHUB_SHA / $GITHUB_REF_NAME) -- avoid edge cases with
 *      shallow clones, detached HEADs, etc. in CI.
 *   2. Local `git rev-parse` against the parent repo (../..).
 *   3. Fallback stamp for local/personal builds from non-git source trees
 *      (ZIP extract, interrupted clone with no HEAD, etc.).
 *
 * Dev / out-of-repo builds without git produce an explicit fallback stamp
 * rather than aborting the whole build.  Bootstrap treats the all-zero
 * commit as unpinned and follows the branch instead of fetching a fake SHA.
 */

import { createHash } from "crypto"
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs"
import { resolve, join, relative, delimiter } from "path"
import { execSync, execFileSync } from "child_process"

import { isMain } from "./utils.mjs"

const STAMP_SCHEMA_VERSION = 1
export const AGENT_OS_NATIVE_PROMPT_MAX_BYTES = 4096
export const AGENT_OS_RUNTIME_FILES = Object.freeze([
  "AGENT_OS_CORE.md",
  "agent/prompt_builder.py",
  "agent/agent_os_install.py",
])
export const AGENT_OS_SEMANTIC_TERMS = Object.freeze([
  "GOAL",
  "CAPABILITY_ID",
  "PRIOR_WORK_LOCATE",
  "ACCEPTED_BASELINE",
  "FIRST_UNPROVEN_BOUNDARY",
  "INVALIDATOR_CHECK",
  "READY_FRONTIER",
  "CRITICAL_PATH",
  "PARALLEL_SAFE_FRONTIER",
  "BROKER_UNKNOWN",
])
export const AGENT_OS_WRITER_TERM = "ONE_PRIMARY_WRITER"

/** All-zero placeholder used when no real commit can be resolved. */
export const FALLBACK_COMMIT = "0000000000000000000000000000000000000000"
export const FALLBACK_BRANCH = "main"

const DESKTOP_ROOT = resolve(import.meta.dirname, "..")
const REPO_ROOT = resolve(DESKTOP_ROOT, "..", "..")
const OUT_DIR = join(DESKTOP_ROOT, "build")
const OUT_FILE = join(OUT_DIR, "install-stamp.json")
const AGENT_OS_RULE_VERSION_RE = /^AGENT_OS_RULE_VERSION:\s*(\S+)\s*$/m

function sha256Text(value) {
  return createHash("sha256").update(value, "utf8").digest("hex")
}

function resolvePythonInvocation(repoRoot = REPO_ROOT) {
  const candidates = process.platform === "win32"
    ? [
        { command: join(repoRoot, ".venv", "Scripts", "python.exe"), args: [] },
        { command: join(repoRoot, "venv", "Scripts", "python.exe"), args: [] },
        { command: "python", args: [] },
        { command: "py", args: ["-3"] },
      ]
    : [
        { command: join(repoRoot, ".venv", "bin", "python"), args: [] },
        { command: join(repoRoot, "venv", "bin", "python"), args: [] },
        { command: "python3", args: [] },
        { command: "python", args: [] },
      ]

  for (const candidate of candidates) {
    if (!candidate.command.includes("/") && !candidate.command.includes("\\")) {
      return candidate
    }
    if (existsSync(candidate.command)) {
      return candidate
    }
  }

  return candidates[candidates.length - 1]
}

export function renderAgentOsRuntimePrompt({
  repoRoot = REPO_ROOT,
  execFileSyncFn = execFileSync,
} = {}) {
  const repoPath = resolve(repoRoot)
  const python = resolvePythonInvocation(repoPath)
  const env = {
    ...process.env,
    PYTHONPATH: [repoPath, process.env.PYTHONPATH].filter(Boolean).join(delimiter),
  }
  const script = [
    "import json",
    "from agent.agent_os_install import MANAGED_BEGIN, MANAGED_END, render_hermes_native_prompt",
    "payload = {",
    "  'prompt': render_hermes_native_prompt('.'),",
    "  'managed_begin': MANAGED_BEGIN,",
    "  'managed_end': MANAGED_END,",
    "}",
    "print(json.dumps(payload))",
  ].join("\n")
  const raw = execFileSyncFn(python.command, [...python.args, "-c", script], {
    cwd: repoPath,
    env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim()

  return JSON.parse(raw)
}

export function buildAgentOsRuntimeBinding({
  repoRoot = REPO_ROOT,
  readTextFn = (filePath) => readFileSync(filePath, "utf8"),
  renderPromptFn = (root) => renderAgentOsRuntimePrompt({ repoRoot: root }),
} = {}) {
  const repoPath = resolve(repoRoot)
  const files = AGENT_OS_RUNTIME_FILES.map((relPath) => {
    const content = readTextFn(join(repoPath, relPath))
    return {
      path: relPath,
      sha256: sha256Text(content),
      bytes: Buffer.byteLength(content, "utf8"),
    }
  })
  const promptPayload = renderPromptFn(repoPath)
  const prompt = String(promptPayload?.prompt || "")
  const ruleVersion = (prompt.match(AGENT_OS_RULE_VERSION_RE) || [null, null])[1]
  const semanticVisibility = Object.fromEntries(
    AGENT_OS_SEMANTIC_TERMS.map((term) => [term, prompt.includes(term)])
  )
  const managedBlock = {
    begin: String(promptPayload?.managed_begin || ""),
    end: String(promptPayload?.managed_end || ""),
  }
  const writerVisible = prompt.includes(AGENT_OS_WRITER_TERM)
  const managedBlockVisible = files
    .find((entry) => entry.path === "agent/agent_os_install.py")
    ? (() => {
        const source = readTextFn(join(repoPath, "agent/agent_os_install.py"))
        return source.includes(managedBlock.begin) && source.includes(managedBlock.end)
      })()
    : false
  const nativePromptBytes = Buffer.byteLength(prompt, "utf8")

  return {
    files,
    source_identity_sha256: sha256Text(
      JSON.stringify({
        files,
        ruleVersion,
        prompt,
      })
    ),
    agent_os_rule_version: ruleVersion,
    agent_os_native_prompt_sha256: sha256Text(prompt),
    agent_os_native_prompt_bytes: nativePromptBytes,
    agent_os_native_prompt_max_bytes: AGENT_OS_NATIVE_PROMPT_MAX_BYTES,
    agent_os_semantic_terms: AGENT_OS_SEMANTIC_TERMS,
    agent_os_semantic_visibility: semanticVisibility,
    agent_os_writer_term: AGENT_OS_WRITER_TERM,
    agent_os_writer_visible: writerVisible,
    agent_os_managed_block: managedBlock,
    agent_os_managed_block_visible: managedBlockVisible,
    agent_os_prompt_bloat_ok: nativePromptBytes <= AGENT_OS_NATIVE_PROMPT_MAX_BYTES,
  }
}

function tryExec(cmd, opts) {
  try {
    return execSync(cmd, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], ...opts }).trim()
  } catch {
    return null
  }
}

export function fromCI(env = process.env) {
  const sha = env.GITHUB_SHA
  if (!sha) return null
  const branch = env.GITHUB_REF_NAME || env.GITHUB_HEAD_REF || null
  return {
    commit: sha,
    branch: branch,
    dirty: false, // CI builds from a checkout-of-ref by definition
    source: "ci"
  }
}

export function fromLocalGit(repoRoot = REPO_ROOT, execFn = tryExec) {
  const sha = execFn("git rev-parse HEAD", { cwd: repoRoot })
  if (!sha) return null
  const branch = execFn("git rev-parse --abbrev-ref HEAD", { cwd: repoRoot })
  // `git status --porcelain -uno` is empty iff tracked files match HEAD.
  // We exclude untracked files (-uno) intentionally: a developer who's
  // checked out an installer scratch dir alongside the repo shouldn't
  // poison every local build with a [DIRTY] stamp.  We DO care about
  // tracked-but-modified files because those mean the .exe content
  // differs from the commit being pinned.
  const status = execFn("git status --porcelain -uno", { cwd: repoRoot })
  const dirty = status !== null && status.length > 0
  return {
    commit: sha,
    branch: branch === "HEAD" ? null : branch, // detached HEAD -> null
    dirty: dirty,
    source: "local"
  }
}

export function fromFallback(branch = FALLBACK_BRANCH) {
  // Non-git builds (ZIP download, bootstrap installer without a resolvable
  // HEAD) cannot determine a real commit.  Use a placeholder so local /
  // personal builds can still complete.  The desktop bootstrap treats the
  // all-zero commit as "unknown" and falls back to an unpinned branch
  // bootstrap instead of trying to fetch a non-existent GitHub commit.
  return {
    commit: FALLBACK_COMMIT,
    branch: branch || FALLBACK_BRANCH,
    dirty: false,
    source: "fallback"
  }
}

/**
 * Resolve the install stamp without writing it.  Pure enough for unit tests:
 * inject env / execFn / repoRoot to simulate CI, local git, or no-git trees.
 */
export function resolveStamp({
  env = process.env,
  repoRoot = REPO_ROOT,
  execFn = tryExec,
  fallbackBranch = FALLBACK_BRANCH
} = {}) {
  return fromCI(env) || fromLocalGit(repoRoot, execFn) || fromFallback(fallbackBranch)
}

export function isFallbackCommit(commit) {
  return typeof commit === "string" && /^0{7,40}$/.test(commit)
}

function main() {
  const stamp = resolveStamp()
  if (!stamp || !stamp.commit) {
    // Should not happen — fromFallback() always provides a commit.
    console.error(
      "[write-build-stamp] ERROR: could not determine git commit.\n" +
        "  - $GITHUB_SHA not set\n" +
        "  - `git rev-parse HEAD` failed at " +
        REPO_ROOT +
        "\n" +
        "Packaged builds require a git ref to pin first-launch install.ps1\n" +
        "against. Run from a git checkout or set $GITHUB_SHA explicitly."
    )
    process.exit(1)
  }

  if (isFallbackCommit(stamp.commit)) {
    console.warn(
      "[write-build-stamp] WARNING: no git commit found (non-git checkout?).\n" +
        "  Using placeholder commit — the packaged app will fall back to the\n" +
        "  default branch for first-launch bootstrap.  For production builds,\n" +
        "  run from a git checkout or set $GITHUB_SHA."
    )
  }

  if (stamp.dirty) {
    console.warn(
      "[write-build-stamp] WARNING: working tree is dirty.\n" +
        "  Pinning to " +
        stamp.commit.slice(0, 12) +
        " but the packaged code may differ from that commit.\n" +
        "  Commit your changes before publishing this build."
    )
  }

  const payload = {
    schemaVersion: STAMP_SCHEMA_VERSION,
    commit: stamp.commit,
    branch: stamp.branch,
    builtAt: new Date().toISOString(),
    dirty: stamp.dirty,
    source: stamp.source,
    runtimeBinding: buildAgentOsRuntimeBinding(),
  }

  mkdirSync(OUT_DIR, { recursive: true })
  writeFileSync(OUT_FILE, JSON.stringify(payload, null, 2) + "\n", "utf8")
  console.log(
    "[write-build-stamp] wrote " +
      relative(REPO_ROOT, OUT_FILE) +
      " -> " +
      stamp.commit.slice(0, 12) +
      (stamp.branch ? " (" + stamp.branch + ")" : "") +
      (stamp.dirty ? " [DIRTY]" : "") +
      (stamp.source === "fallback" ? " [FALLBACK]" : "")
  )
}

if (isMain(import.meta.url)) {
  main()
}
