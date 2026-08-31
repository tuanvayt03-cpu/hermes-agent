import assert from 'node:assert/strict'
import { test } from 'vitest'
import path from 'node:path'

import {
  AGENT_OS_NATIVE_PROMPT_MAX_BYTES,
  AGENT_OS_RUNTIME_FILES,
  AGENT_OS_SEMANTIC_TERMS,
  AGENT_OS_WRITER_TERM,
  buildAgentOsRuntimeBinding,
  FALLBACK_BRANCH,
  FALLBACK_COMMIT,
  fromCI,
  fromFallback,
  fromLocalGit,
  isFallbackCommit,
  renderAgentOsRuntimePrompt,
  resolveStamp
} from './write-build-stamp.mjs'

test('fromCI reads GITHUB_SHA / GITHUB_REF_NAME', () => {
  assert.deepEqual(
    fromCI({ GITHUB_SHA: 'a'.repeat(40), GITHUB_REF_NAME: 'release' }),
    { commit: 'a'.repeat(40), branch: 'release', dirty: false, source: 'ci' }
  )
  assert.equal(fromCI({}), null)
})

test('fromLocalGit returns null when git rev-parse fails', () => {
  const stamp = fromLocalGit('/tmp/not-a-repo', () => null)
  assert.equal(stamp, null)
})

test('fromLocalGit reads HEAD + branch + dirty status', () => {
  const calls = []
  const execFn = (cmd) => {
    calls.push(cmd)
    if (cmd === 'git rev-parse HEAD') return 'b'.repeat(40)
    if (cmd === 'git rev-parse --abbrev-ref HEAD') return 'main'
    if (cmd === 'git status --porcelain -uno') return ' M apps/desktop/package.json'
    return null
  }
  assert.deepEqual(fromLocalGit('/repo', execFn), {
    commit: 'b'.repeat(40),
    branch: 'main',
    dirty: true,
    source: 'local'
  })
  assert.ok(calls.includes('git rev-parse HEAD'))
})

test('fromFallback uses the all-zero placeholder commit', () => {
  assert.deepEqual(fromFallback(), {
    commit: FALLBACK_COMMIT,
    branch: FALLBACK_BRANCH,
    dirty: false,
    source: 'fallback'
  })
  assert.equal(isFallbackCommit(FALLBACK_COMMIT), true)
  assert.equal(isFallbackCommit('a'.repeat(40)), false)
})

test('resolveStamp prefers CI over local git over fallback', () => {
  const ci = resolveStamp({
    env: { GITHUB_SHA: 'c'.repeat(40), GITHUB_REF_NAME: 'main' },
    execFn: () => 'should-not-run'
  })
  assert.equal(ci.source, 'ci')
  assert.equal(ci.commit, 'c'.repeat(40))

  const local = resolveStamp({
    env: {},
    execFn: (cmd) => {
      if (cmd === 'git rev-parse HEAD') return 'd'.repeat(40)
      if (cmd === 'git rev-parse --abbrev-ref HEAD') return 'main'
      if (cmd === 'git status --porcelain -uno') return ''
      return null
    }
  })
  assert.equal(local.source, 'local')
  assert.equal(local.commit, 'd'.repeat(40))
  assert.equal(local.dirty, false)
})

test('resolveStamp falls back when neither CI nor git is available', () => {
  const stamp = resolveStamp({ env: {}, execFn: () => null })
  assert.deepEqual(stamp, {
    commit: FALLBACK_COMMIT,
    branch: FALLBACK_BRANCH,
    dirty: false,
    source: 'fallback'
  })
})

test('buildAgentOsRuntimeBinding carries exact native source and prompt proof', () => {
  const repoRoot = path.resolve('/repo')
  const files = {
    [path.join(repoRoot, 'AGENT_OS_CORE.md')]: 'AGENT_OS_RULE_VERSION: AGENT-OS-V1-20260831-7F3C72A1\n',
    [path.join(repoRoot, 'agent', 'prompt_builder.py')]: 'prompt builder source\n',
    [path.join(repoRoot, 'agent', 'agent_os_install.py')]:
      'MANAGED_BEGIN = "<!-- AGENT_OS_MANAGED_BLOCK_BEGIN -->"\n' +
      'MANAGED_END = "<!-- AGENT_OS_MANAGED_BLOCK_END -->"\n'
  }
  const prompt = [
    'AGENT_OS_RULE_VERSION: AGENT-OS-V1-20260831-7F3C72A1',
    'GOAL CAPABILITY_ID PRIOR_WORK_LOCATE ACCEPTED_BASELINE FIRST_UNPROVEN_BOUNDARY INVALIDATOR_CHECK',
    'READY_FRONTIER CRITICAL_PATH PARALLEL_SAFE_FRONTIER BROKER_UNKNOWN',
    AGENT_OS_WRITER_TERM
  ].join('\n')

  const binding = buildAgentOsRuntimeBinding({
    repoRoot,
    readTextFn: filePath => {
      const value = files[filePath]
      assert.ok(value, `unexpected file read: ${filePath}`)
      return value
    },
    renderPromptFn: () => ({
      prompt,
      managed_begin: '<!-- AGENT_OS_MANAGED_BLOCK_BEGIN -->',
      managed_end: '<!-- AGENT_OS_MANAGED_BLOCK_END -->'
    })
  })

  assert.deepEqual(
    binding.files.map(entry => entry.path),
    AGENT_OS_RUNTIME_FILES
  )
  assert.equal(binding.agent_os_rule_version, 'AGENT-OS-V1-20260831-7F3C72A1')
  assert.equal(binding.agent_os_writer_visible, true)
  assert.equal(binding.agent_os_managed_block_visible, true)
  assert.equal(binding.agent_os_prompt_bloat_ok, true)
  assert.ok(binding.agent_os_native_prompt_bytes <= AGENT_OS_NATIVE_PROMPT_MAX_BYTES)
  for (const term of AGENT_OS_SEMANTIC_TERMS) {
    assert.equal(binding.agent_os_semantic_visibility[term], true, `expected semantic term ${term}`)
  }
  assert.match(binding.source_identity_sha256, /^[a-f0-9]{64}$/)
  assert.match(binding.agent_os_native_prompt_sha256, /^[a-f0-9]{64}$/)
})

test('renderAgentOsRuntimePrompt renders the live native Agent OS prompt', () => {
  const rendered = renderAgentOsRuntimePrompt()

  assert.match(rendered.prompt, /^AGENT_OS_RULE_VERSION:\s+AGENT-OS-V1-20260831-7F3C72A1/m)
  assert.equal(rendered.managed_begin, '<!-- AGENT_OS_MANAGED_BLOCK_BEGIN -->')
  assert.equal(rendered.managed_end, '<!-- AGENT_OS_MANAGED_BLOCK_END -->')
  for (const term of AGENT_OS_SEMANTIC_TERMS) {
    assert.match(rendered.prompt, new RegExp(term))
  }
  assert.match(rendered.prompt, new RegExp(AGENT_OS_WRITER_TERM))
})
