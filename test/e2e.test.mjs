// End-to-end tests on a real n8n (npm run test:e2e).
//
// Every structure is run over its webhook against a Postgres table of 137
// rows: once clean, and once per stage with an interruption in the middle of
// that stage followed by a second run (the resume). What counts is the
// stand-in's own record: how often each stage sub-workflow was called, with
// how many rows, and how often each row was worked on by each stage. The table
// is read back at the end to see whether a row was left behind.
//
// The tables below pin what n8n 2.47.7 did. If a later version behaves
// differently, a test fails and tells you what changed. MEASURE=1 prints the
// numbers without comparing them.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { after, before, test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const BASE = process.env.N8N_URL ?? 'http://localhost:55693';
const STANDIN = process.env.STANDIN_URL ?? 'http://localhost:58093';
const MEASURE = Boolean(process.env.MEASURE);
const root = join(dirname(fileURLToPath(import.meta.url)), '..');

const ROWS = 137;
const STAGES = ['vectorize', 'duplicate', 'relevancy'];
const GIVES = { vectorize: ['vectorize'], duplicate: ['duplicate', 'unique'], relevancy: ['relevant', 'irrelevant'] };
// Where each stage is interrupted: its work call fails after this many rows.
// 123 in Duplicate Finder leaves the 14 + 123 split of the thread's screenshot.
const INTERRUPT = { vectorize: 50, duplicate: 123, relevancy: 60 };

// Per structure and scenario: [calls of the first run, calls of the second
// run, rows a stage worked on twice, rows left without a stage's tag]. The
// second run is the resume, or after a clean run a run with nothing left to do.
// Calls read "stage rows-per-call"; "-" is not called; "137+123" is two calls.
const calls = (vectorize, duplicate, relevancy) => `vectorize ${vectorize} | duplicate ${duplicate} | relevancy ${relevancy}`;
const NONE = 'none';
// What the first run did before it was stopped. The call records the rows it
// was handed, not the rows it finished.
const FIRST = { clean: calls(137, 137, 137), vectorize: calls(137, '-', '-'), duplicate: calls(137, 137, '-'), relevancy: calls(137, 137, 137) };
// Every stage once, with exactly the rows that still lack its tag.
const ONCE = {
  clean: [FIRST.clean, calls('-', '-', '-'), NONE, NONE],
  vectorize: [FIRST.vectorize, calls(87, 137, 137), NONE, NONE],
  duplicate: [FIRST.duplicate, calls('-', 14, 137), NONE, NONE],
  relevancy: [FIRST.relevancy, calls('-', '-', 77), NONE, NONE],
};
// The same for sub-workflows that read their own work list: they are always
// called, with 0 rows when there is nothing to do.
const zero = (text) => text.replaceAll(' -', ' 0');
const ONCE_QUEUES = Object.fromEntries(Object.entries(ONCE).map(([name, [first, second, ...rest]]) => [name, [first, zero(second), ...rest]]));

const EXPECT = {
  // The graph of the thread: a stage runs twice, and its rows are worked on twice.
  'text/loop-back': {
    clean: ONCE.clean,
    vectorize: [FIRST.vectorize, calls(87, '137+50', 137), 'duplicate 50', NONE],
    duplicate: [FIRST.duplicate, calls('-', 14, '137+123'), 'relevancy 123', NONE],
    relevancy: ONCE.relevancy,
  },
  // IF per stage, no Merge: two calls, but with different rows.
  'text/if-chain': {
    clean: ONCE.clean,
    vectorize: [FIRST.vectorize, calls(87, '87+50', '87+50'), NONE, NONE],
    duplicate: [FIRST.duplicate, calls('-', 14, '14+123'), NONE, NONE],
    relevancy: ONCE.relevancy,
  },
  // A Select per stage in the parent: stops at the first Select without rows.
  'text/select-chain': {
    clean: ONCE.clean,
    vectorize: ONCE.vectorize,
    duplicate: [FIRST.duplicate, calls('-', '-', '-'), NONE, 'duplicate 14 | relevancy 137'],
    relevancy: [FIRST.relevancy, calls('-', '-', '-'), NONE, 'relevancy 77'],
  },
  'text/skip-and-merge': ONCE,
  'text/stage-queues': ONCE_QUEUES,
  'jsonb/skip-and-merge': ONCE,
  'jsonb/stage-queues': ONCE_QUEUES,
};
// These must hold their promise in every scenario, whatever the pinned numbers say.
const FIXED = ['text/skip-and-merge', 'text/stage-queues', 'jsonb/skip-and-merge', 'jsonb/stage-queues'];

const findings = [];

// ---- talking to the stack ------------------------------------------------------

const post = (url, body = {}) => fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
const standin = async (path, body) => (await (body ? post(`${STANDIN}${path}`, body) : fetch(`${STANDIN}${path}`))).json();

// n8n's port accepts connections before n8n answers, and it serves a
// "starting up" page before the webhooks are registered. Only a real answer of
// the admin workflow counts as ready; everything else is tried again.
async function ready(timeout = 420_000) {
  const deadline = Date.now() + timeout;
  for (;;) {
    try {
      const response = await post(`${BASE}/webhook/stages/admin/state`, { store: 'text' });
      const rows = response.status === 200 ? JSON.parse(await response.text()) : null;
      if (Array.isArray(rows) && rows.length > 0 && Array.isArray(rows[0].status)) return;
    } catch {
      // connection refused or reset, or not JSON yet
    }
    if (Date.now() > deadline) throw new Error('n8n did not come up');
    await sleep(2000);
  }
}

async function seed(store) {
  const response = await post(`${BASE}/webhook/stages/admin/seed`, { store, rows: ROWS });
  assert.equal(response.status, 200, `seed ${store}: ${await response.clone().text()}`);
  assert.deepEqual(await response.json(), { rows: ROWS });
}
async function state(store) {
  const response = await post(`${BASE}/webhook/stages/admin/state`, { store });
  assert.equal(response.status, 200, `state ${store}`);
  return response.json();
}

// One run of a structure. n8n answers 500 "No item to return was found" when
// the last node had no output; only "Error in workflow" means the run failed.
async function run(path) {
  const response = await post(`${BASE}/webhook/stages/${path}`);
  const text = await response.text();
  return { failed: response.status === 500 && text.includes('Error in workflow'), status: response.status, text };
}

// ---- reading the stand-in's record ----------------------------------------------

const callsText = (made) =>
  STAGES.map((stage) => {
    const sizes = made.filter((call) => call.stage === stage).map((call) => call.items);
    return `${stage} ${sizes.length ? sizes.join('+') : '-'}`;
  }).join(' | ');

// Rows a stage worked on more than once (calls that were refused do not count).
function twice(work) {
  const parts = [];
  for (const stage of STAGES) {
    const seen = new Map();
    for (const entry of work) if (entry.stage === stage && entry.ok) seen.set(entry.id, (seen.get(entry.id) ?? 0) + 1);
    const rows = [...seen.values()].filter((count) => count > 1).length;
    if (rows) parts.push(`${stage} ${rows}`);
  }
  return parts.join(' | ') || NONE;
}
// Rows that end without a stage's tag.
function left(rows) {
  const parts = [];
  for (const stage of STAGES) {
    const missing = rows.filter((row) => !row.status.some((tag) => GIVES[stage].includes(tag))).length;
    if (missing) parts.push(`${stage} ${missing}`);
  }
  return parts.join(' | ') || NONE;
}
// No call may carry the same row twice.
const noCallHasDuplicates = (made) => made.every((call) => new Set(call.ids).size === call.ids.length);

async function scenario(path, interrupt) {
  const [store] = path.split('/');
  await seed(store);
  await standin('/reset', interrupt ? { failStage: interrupt, failAfter: INTERRUPT[interrupt] } : {});

  const first = await run(path);
  const afterFirst = await standin('/report');
  if (interrupt) assert.equal(first.failed, true, `${path}: the run interrupted in ${interrupt} must fail, got ${first.status} ${first.text.slice(0, 200)}`);
  else assert.equal(first.failed, false, `${path}: the clean run failed: ${first.text.slice(0, 300)}`);
  const second = await run(path);
  assert.equal(second.failed, false, `${path}: the second run failed: ${second.text.slice(0, 300)}`);
  const resumedCalls = (await standin('/report')).calls.slice(afterFirst.calls.length);
  const report = await standin('/report');
  const rows = await state(store);
  assert.equal(rows.length, ROWS);
  return {
    firstCalls: afterFirst.calls,
    resumedCalls,
    result: [callsText(afterFirst.calls), callsText(resumedCalls), twice(report.work), left(rows)],
    report,
  };
}

// ---- tests ------------------------------------------------------------------------

before(() => ready());

for (const [path, scenarios] of Object.entries(EXPECT)) {
  for (const name of ['clean', ...STAGES]) {
    const title = name === 'clean' ? `${path}: clean run, then run again with nothing left to do` : `${path}: interrupted in ${name} after ${INTERRUPT[name]} rows, then run again`;
    test(title, async () => {
      const { result, firstCalls, resumedCalls } = await scenario(path, name === 'clean' ? null : name);
      findings.push({ structure: path, scenario: name === 'clean' ? 'clean, then again' : `interrupted in ${name} after ${INTERRUPT[name]} rows`, first: result[0], resumed: result[1], twice: result[2], left: result[3] });

      if (FIXED.includes(path)) {
        for (const made of [firstCalls, resumedCalls]) {
          for (const stage of STAGES) assert.ok(made.filter((call) => call.stage === stage).length <= 1, `${path}: ${stage} was called more than once in one run: ${callsText(made)}`);
          assert.ok(noCallHasDuplicates(made), `${path}: a call got the same row twice`);
        }
        assert.equal(result[2], NONE, `${path}: rows worked on twice by one stage`);
        assert.equal(result[3], NONE, `${path}: rows left behind`);
      }
      if (!MEASURE) assert.deepEqual(result, scenarios[name], `${path}, ${name}: [first run, resumed run, rows worked twice, rows left]`);
    });
  }
}

// A real interruption: the n8n container is killed while Duplicate Finder is
// halfway through, started again, and the workflow is run a second time.
for (const path of ['text/skip-and-merge', 'text/stage-queues']) {
  test(`${path}: n8n killed in the middle of Duplicate Finder, started again, run again`, async () => {
    const stack = (action) => assert.equal(spawnSync('sh', ['test/stack.sh', action], { cwd: root, stdio: 'ignore' }).status, 0, `test/stack.sh ${action}`);
    await seed('text');
    await standin('/reset', { delayMs: 40 });

    const first = run(path).catch((error) => ({ killed: true, error: String(error) }));
    for (let tries = 0; ; tries++) {
      const { work } = await standin('/report');
      if (work.filter((entry) => entry.stage === 'duplicate').length >= 30) break;
      assert.ok(tries < 600, 'Duplicate Finder never got to row 30');
      await sleep(50);
    }
    stack('kill');
    const outcome = await first;
    assert.ok(outcome.killed || outcome.status !== 200, 'the killed run must not answer 200');

    stack('start');
    await ready(180_000);
    // n8n does not pick the dead run up again by itself.
    const atKill = await standin('/report');
    await sleep(5000);
    const later = await standin('/report');
    assert.equal(later.work.length, atKill.work.length, 'n8n went on with the killed run by itself');

    const tagged = (await state('text')).filter((row) => row.status.some((tag) => GIVES.duplicate.includes(tag))).length;
    assert.ok(tagged >= 29 && tagged < ROWS, `rows tagged by Duplicate Finder at the kill: ${tagged}`);

    await standin('/reset', { keep: true });
    const second = await run(path);
    assert.equal(second.failed, false, `the run after the kill failed: ${second.text.slice(0, 300)}`);
    const report = await standin('/report');
    const resumed = report.calls.slice(atKill.calls.length);
    const expected = calls(path.endsWith('stage-queues') ? 0 : '-', ROWS - tagged, ROWS);
    assert.equal(callsText(resumed), expected, 'calls of the run after the kill');
    assert.equal(left(await state('text')), NONE, 'rows left behind');

    // The row that was in flight at the kill was seen by the stand-in but not
    // tagged, so it is worked on again. No other row may be.
    const again = twice(report.work);
    assert.ok(again === NONE || again === 'duplicate 1', `rows worked on twice: ${again}`);
    findings.push({ structure: path, scenario: `n8n killed in duplicate after ${tagged} tagged rows`, first: callsText(atKill.calls), resumed: callsText(resumed), twice: again, left: NONE });
  });
}

after(() => {
  console.log('\nWhat was measured ("stage rows-per-call"; "-" = not called; "a+b" = two calls)\n');
  for (const row of findings) {
    console.log(`${row.structure} | ${row.scenario}`);
    console.log(`    first run:  ${row.first}`);
    console.log(`    second run: ${row.resumed}`);
    console.log(`    rows worked on twice by a stage: ${row.twice}    rows left behind: ${row.left}`);
  }
});
