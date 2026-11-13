// Builds the workflow files:
//   workflows/text-array/   for a `status text[]` column
//   workflows/jsonb/        for a `status jsonb` column holding an array of strings
//     skip-and-merge.json     read once; per stage IF -> sub-workflow -> Merge
//     stage-queues.json       three sub-workflow calls in a row; each stage reads its own work list
//     stage-*.json            stand-in stages that work on the rows they are handed
//     queue-stage-*.json      stand-in stages that read their own work list
//   lab/                    the structures that were measured against them (text[] only)
//     loop-back.json          the graph of the thread: Select -> Switch -> stages -> back to Select
//     if-chain.json           IF -> stage, both wires straight into the next IF (no Merge)
//     select-chain.json       Select -> stage -> Select -> stage -> Select -> stage
//     admin.json              seed and read the table, for the tests
//
// The results are plain n8n exports: import them and they run, no build
// needed. This script exists so both column types share one description.
// After a change:   node scripts/build.mjs

import { createHash } from 'node:crypto';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const STANDIN = 'http://standin:8080';
const POSTGRES = { postgres: { id: 'stagesPostgres', name: 'Stages Postgres' } };

// The pipeline of the thread. A row is done with a stage when it carries one
// of the tags the stage gives; it is ready for a stage when it carries one of
// the tags the stage before it gives.
const STAGES = [
  { key: 'vectorize', name: 'Vectorize News Record', after: ['candidate'], gives: ['vectorize'] },
  { key: 'duplicate', name: 'Duplicate Finder', after: ['vectorize'], gives: ['duplicate', 'unique'] },
  { key: 'relevancy', name: 'Event Relevancy Screening', after: ['duplicate', 'unique'], gives: ['relevant', 'irrelevant'] },
];

const list = (tags) => tags.map((tag) => `'${tag}'`).join(', ');
const STORES = {
  text: {
    folder: 'text-array',
    label: 'text[]',
    table: 'events',
    any: (tags) => `status && ARRAY[${list(tags)}]`,
    tag: [
      'UPDATE events',
      'SET status = CASE WHEN status @> ARRAY[$2::text] THEN status ELSE array_append(status, $2::text) END',
      'WHERE id = $1',
      'RETURNING id, title, status',
    ],
  },
  jsonb: {
    folder: 'jsonb',
    label: 'jsonb',
    table: 'events_jsonb',
    any: (tags) => `status ?| ARRAY[${list(tags)}]`,
    tag: [
      'UPDATE events_jsonb',
      'SET status = CASE WHEN status @> to_jsonb($2::text) THEN status ELSE status || to_jsonb($2::text) END',
      'WHERE id = $1',
      'RETURNING id, title, status',
    ],
  },
};
const selectAll = (store) => [`SELECT id, title, status FROM ${store.table} ORDER BY id`];
// "Rows that are ready for this stage and do not have its tag yet."
const workList = (store, stage) => [
  'SELECT id, title, status',
  `FROM ${store.table}`,
  `WHERE ${store.any(stage.after)}`,
  `  AND NOT ${store.any(stage.gives)}`,
  'ORDER BY id',
];
// The same test on an item, for IF and Switch. The Postgres node hands both
// column types to n8n as an array of strings.
const lacks = (stage) => `={{ !$json.status.some((tag) => [${list(stage.gives)}].includes(tag)) }}`;

const uuid = (seed) => {
  const hex = createHash('sha1').update(seed).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};
const cap = (text) => text[0].toUpperCase() + text.slice(1);

// ---- a small workflow writer -------------------------------------------------

function workflow(id, name, build) {
  const nodes = [];
  const connections = {};
  const add = (nodeName, type, typeVersion, position, parameters, extra = {}) => {
    nodes.push({ parameters, type: `n8n-nodes-base.${type}`, typeVersion, position, id: uuid(`${id}:${nodeName}`), name: nodeName, ...extra });
    return nodeName;
  };
  const condition = (nodeName, leftValue) => ({
    options: { caseSensitive: true, leftValue: '', typeValidation: 'strict', version: 2 },
    conditions: [{ id: uuid(`${id}:${nodeName}:${leftValue}`), leftValue, rightValue: '', operator: { type: 'boolean', operation: 'true', singleValue: true } }],
    combinator: 'and',
  });
  const w = {
    // wire('A', 'B') or wire('A', 'B', { from: 1, to: 1 })
    wire(source, target, { from = 0, to = 0 } = {}) {
      const outputs = ((connections[source] ??= { main: [] }).main);
      while (outputs.length <= from) outputs.push([]);
      outputs[from].push({ node: target, type: 'main', index: to });
    },
    chain(...names) {
      for (let i = 1; i < names.length; i++) w.wire(names[i - 1], names[i]);
    },
    note(position, width, height, lines) {
      return add(`About ${nodes.length}`, 'stickyNote', 1, position, { content: lines.join('\n'), height, width });
    },
    webhook(nodeName, position, path, options = {}) {
      return add(nodeName, 'webhook', 2.1, position, { httpMethod: 'POST', path, responseMode: 'lastNode', options: {}, ...options }, { webhookId: uuid(`webhook:${path}`) });
    },
    code(nodeName, position, lines) {
      return add(nodeName, 'code', 2, position, { jsCode: lines.join('\n') });
    },
    sql(nodeName, position, query, replacement, extra = {}) {
      return add(
        nodeName,
        'postgres',
        2.6,
        position,
        { operation: 'executeQuery', query: query.join('\n'), options: replacement ? { queryReplacement: `={{ ${replacement} }}` } : {} },
        { credentials: POSTGRES, ...extra },
      );
    },
    post(nodeName, position, path, body) {
      return add(nodeName, 'httpRequest', 4.2, position, {
        method: 'POST',
        url: `${STANDIN}${path}`,
        sendBody: true,
        specifyBody: 'json',
        jsonBody: `={{ JSON.stringify(${body}) }}`,
        options: {},
      });
    },
    iff(nodeName, position, leftValue) {
      return add(nodeName, 'if', 2.2, position, { conditions: condition(nodeName, leftValue), options: {} });
    },
    switchOn(nodeName, position, rules) {
      return add(nodeName, 'switch', 3.2, position, {
        rules: { values: rules.map(([outputKey, leftValue]) => ({ conditions: condition(nodeName, leftValue), renameOutput: true, outputKey })) },
        options: {},
      });
    },
    merge(nodeName, position) {
      return add(nodeName, 'merge', 3.2, position, {});
    },
    loop(nodeName, position) {
      return add(nodeName, 'splitInBatches', 3, position, { batchSize: 1, options: {} });
    },
    pass(nodeName, position) {
      return add(nodeName, 'noOp', 1, position, {});
    },
    called(nodeName, position) {
      return add(nodeName, 'executeWorkflowTrigger', 1.1, position, { inputSource: 'passthrough' });
    },
    sub(nodeName, position, workflowId, extra = {}) {
      return add(
        nodeName,
        'executeWorkflow',
        1.2,
        position,
        {
          workflowId: { __rl: true, mode: 'id', value: workflowId },
          workflowInputs: { mappingMode: 'defineBelow', value: {}, matchingColumns: [], schema: [], attemptToConvertTypes: false, convertFieldsToString: true },
          mode: 'once',
          options: {},
        },
        extra,
      );
    },
  };
  build(w);
  return { name, nodes, pinData: {}, connections, active: false, settings: { executionOrder: 'v1' }, id };
}

const ids = (slug) => ({
  stage: (stage) => `stages${cap(slug)}Stage${cap(stage.key)}`,
  queue: (stage) => `stages${cap(slug)}Queue${cap(stage.key)}`,
  parent: (kind) => `stages${cap(slug)}${kind}`,
});

// ---- the stand-in stages -------------------------------------------------------
// Both kinds do the same per row: one call to the stand-in (the "work"), then
// the tag is written to that row. The tag is saved row by row, so a run that
// stops halfway leaves the finished rows tagged.

function rowLoop(w, store, stage, x, y) {
  w.loop('One row at a time', [x, y]);
  w.post('Do the work', [x + 220, y + 100], '/work', `{ stage: '${stage.key}', id: $json.id }`);
  w.sql('Tag the row', [x + 440, y + 100], store.tag, '[$json.id, $json.tag]');
  w.wire('One row at a time', 'Do the work', { from: 1 });
  w.chain('Do the work', 'Tag the row', 'One row at a time');
}

// Works on the rows the parent hands in and returns them with their new tags.
const stageWorkflow = (slug, store, stage) =>
  workflow(ids(slug).stage(stage), `Stages · ${stage.name} (stand-in, ${store.label})`, (w) => {
    w.note([-40, -200], 560, 150, [
      `## ${stage.name} (stand-in)`,
      `Gets rows from the parent, does one piece of "work" per row and adds \`${stage.gives.join('` or `')}\` to that row's \`status\`. Returns the rows with their new status, which is what the next IF in the parent looks at.`,
    ]);
    w.called('When called by the parent', [0, 0]);
    w.code('This call', [220, 0], [`return [{ json: { stage: '${stage.key}', ids: $input.all().map((item) => item.json.id) } }];`]);
    w.post('Record the call', [440, 0], '/call', '$json');
    w.code('Rows', [660, 0], ["return $('When called by the parent').all().map((item) => ({ json: item.json }));"]);
    rowLoop(w, store, stage, 880, 0);
    w.pass('Return the rows', [1100, -100]);
    w.chain('When called by the parent', 'This call', 'Record the call', 'Rows', 'One row at a time', 'Return the rows');
  });

// Reads its own work list, so the parent hands it nothing. Always returns one
// summary item, so the parent goes on to the next stage even with no work.
const queueWorkflow = (slug, store, stage) =>
  workflow(ids(slug).queue(stage), `Stages · ${stage.name} with its own work list (stand-in, ${store.label})`, (w) => {
    w.note([-40, -220], 620, 170, [
      `## ${stage.name} with its own work list (stand-in)`,
      `Ignores what the parent sends. **My work list** selects the rows that carry \`${stage.after.join('` or `')}\` and do not carry \`${stage.gives.join('` or `')}\` yet. Each row is tagged as soon as it is done. Returns one item with the count, also when there was nothing to do.`,
    ]);
    w.called('When called by the parent', [0, 0]);
    w.sql('My work list', [220, 0], workList(store, stage), null, { executeOnce: true, alwaysOutputData: true });
    w.code('This call', [440, 0], [
      '// An empty work list arrives as one empty item (Always Output Data).',
      `return [{ json: { stage: '${stage.key}', ids: $input.all().map((item) => item.json.id).filter((id) => id !== undefined) } }];`,
    ]);
    w.post('Record the call', [660, 0], '/call', '$json');
    w.iff('Anything to do?', [880, 0], "={{ $('This call').first().json.ids.length > 0 }}");
    w.code('Rows', [1100, -100], ["return $('My work list').all().map((item) => ({ json: item.json }));"]);
    rowLoop(w, store, stage, 1320, -100);
    w.code('Report', [1760, 100], [`return [{ json: { stage: '${stage.key}', rows: $('This call').first().json.ids.length } }];`]);
    w.chain('When called by the parent', 'My work list', 'This call', 'Record the call', 'Anything to do?', 'Rows', 'One row at a time', 'Report');
    w.wire('Anything to do?', 'Report', { from: 1 });
  });

// ---- the two structures that keep the skipping and run each stage once ----------

const skipAndMerge = (slug, store) =>
  workflow(ids(slug).parent('SkipAndMerge'), `Stages · Skip and merge (${store.label})`, (w) => {
    w.note([-40, -300], 760, 190, [
      '## Skip and merge',
      'The table is read once. For every stage: rows that already carry its tag take the lower wire, the others go through the sub-workflow, and a **Merge** (Append) joins both before the next stage. So every stage is entered at most once, with exactly the rows that still need it.',
      '',
      'The sub-workflow must return the rows with their new `status`. Replace the Webhook with your own trigger.',
    ]);
    w.webhook('Webhook', [0, 0], `stages/${slug}/skip-and-merge`);
    w.sql('Select rows from a table', [220, 0], selectAll(store), null, { executeOnce: true });
    w.chain('Webhook', 'Select rows from a table');
    let previous = 'Select rows from a table';
    STAGES.forEach((stage, index) => {
      const x = 440 + index * 660;
      const gate = w.iff(`Needs ${stage.name}?`, [x, 0], lacks(stage));
      const child = w.sub(stage.name, [x + 220, -100], ids(slug).stage(stage));
      const joined = w.merge(`After ${stage.name}`, [x + 440, 0]);
      w.wire(previous, gate);
      w.wire(gate, child);
      w.wire(child, joined);
      w.wire(gate, joined, { from: 1, to: 1 });
      previous = joined;
    });
  });

const stageQueues = (slug, store) =>
  workflow(ids(slug).parent('StageQueues'), `Stages · Stage queues (${store.label})`, (w) => {
    w.note([-40, -280], 760, 170, [
      '## Stage queues',
      'Three sub-workflow calls in a row and nothing else. Each sub-workflow starts with its own query ("ready for me, not tagged by me yet"), so the parent has no Select, no Switch and no wire going back. Each sub-workflow returns one summary item, which starts the next one.',
      '',
      'Replace the Webhook with your own trigger.',
    ]);
    w.webhook('Webhook', [0, 0], `stages/${slug}/stage-queues`);
    let previous = 'Webhook';
    STAGES.forEach((stage, index) => {
      // Execute Once: one call, whatever the node before it returned.
      const child = w.sub(stage.name, [220 + index * 220, 0], ids(slug).queue(stage), { executeOnce: true });
      w.wire(previous, child);
      previous = child;
    });
  });

// ---- the structures that were measured against them (text[] only) ---------------

const text = STORES.text;
const textIds = ids('text');

// The graph of the thread, read off the screenshot.
const loopBack = workflow('stagesLabLoopBack', 'Stages lab · Loop back (the graph of the thread)', (w) => {
  w.note([-40, -300], 700, 170, [
    '## Loop back',
    'Select -> Switch -> one sub-workflow per missing tag, and every sub-workflow wired back to Select. On a clean run each stage runs once. After an interruption rows leave the Switch on two outputs at once, both come back, and a stage runs more than once.',
  ]);
  w.webhook('Webhook', [0, 0], 'stages/text/loop-back');
  w.sql('Select rows from a table', [220, 0], selectAll(text), null, { executeOnce: true });
  w.switchOn(
    'Switch',
    [440, 0],
    STAGES.map((stage) => [stage.name, lacks(stage)]),
  );
  w.chain('Webhook', 'Select rows from a table', 'Switch');
  STAGES.forEach((stage, index) => {
    w.sub(stage.name, [700, (index - 1) * 180], textIds.stage(stage));
    w.wire('Switch', stage.name, { from: index });
    w.wire(stage.name, 'Select rows from a table');
  });
});

// Read once, IF per stage, no Merge: the next IF hangs on two wires.
const ifChain = workflow('stagesLabIfChain', 'Stages lab · IF chain without Merge', (w) => {
  w.note([-40, -300], 700, 150, [
    '## IF chain without Merge',
    'Like "Skip and merge", but the sub-workflow and the skip wire both go straight into the next IF. That IF then runs once per wire, and so does the stage behind it.',
  ]);
  w.webhook('Webhook', [0, 0], 'stages/text/if-chain');
  w.sql('Select rows from a table', [220, 0], selectAll(text), null, { executeOnce: true });
  w.chain('Webhook', 'Select rows from a table');
  let sources = [['Select rows from a table', 0]];
  STAGES.forEach((stage, index) => {
    const x = 440 + index * 440;
    const gate = w.iff(`Needs ${stage.name}?`, [x, 0], lacks(stage));
    const child = w.sub(stage.name, [x + 220, -100], textIds.stage(stage));
    for (const [source, from] of sources) w.wire(source, gate, { from });
    w.wire(gate, child);
    sources = [
      [child, 0],
      [gate, 1],
    ];
  });
});

// One Select per stage in the parent, each started by the stage before it.
const selectChain = workflow('stagesLabSelectChain', 'Stages lab · Select chain', (w) => {
  w.note([-40, -280], 700, 150, [
    '## Select chain',
    'Select -> stage -> Select -> stage -> Select -> stage, each Select with its own "ready and not tagged yet" query. A Select that finds no rows returns no items, and nothing after it runs.',
  ]);
  w.webhook('Webhook', [0, 0], 'stages/text/select-chain');
  let previous = 'Webhook';
  for (const [index, stage] of STAGES.entries()) {
    const select = w.sql(`Rows for ${stage.name}`, [220 + index * 440, 0], workList(text, stage), null, { executeOnce: true });
    const child = w.sub(stage.name, [440 + index * 440, 0], textIds.stage(stage));
    w.chain(previous, select, child);
    previous = child;
  }
});

// Seeds and reads the tables for the tests.
const admin = workflow('stagesLabAdmin', 'Stages lab · Admin (seed and read the table)', (w) => {
  const table = (source) => `{{ $('${source}').first().json.body.store === 'jsonb' ? 'events_jsonb' : 'events' }}`;
  w.webhook('Seed', [0, 0], 'stages/admin/seed');
  w.sql('Empty the table', [220, 0], [`=TRUNCATE ${table('Seed')}`], null, { alwaysOutputData: true, executeOnce: true });
  w.sql(
    'Insert candidates',
    [440, 0],
    [
      `=INSERT INTO ${table('Seed')} (id, title, status)`,
      `SELECT n, 'Event ' || n, {{ $('Seed').first().json.body.store === 'jsonb' ? "'[\\"candidate\\"]'::jsonb" : "ARRAY['candidate']" }}`,
      'FROM generate_series(1, $1::int) AS n',
      'RETURNING id',
    ],
    "[$('Seed').first().json.body.rows]",
    { executeOnce: true },
  );
  w.code('Seeded', [660, 0], ['return [{ json: { rows: $input.all().length } }];']);
  w.chain('Seed', 'Empty the table', 'Insert candidates', 'Seeded');

  w.webhook('State', [0, 220], 'stages/admin/state', { responseData: 'allEntries' });
  w.sql('Read the table', [220, 220], [`=SELECT id, status FROM ${table('State')} ORDER BY id`], null, { alwaysOutputData: true });
  w.chain('State', 'Read the table');
});

// ---- write --------------------------------------------------------------------

const files = [
  ['lab/loop-back.json', loopBack],
  ['lab/if-chain.json', ifChain],
  ['lab/select-chain.json', selectChain],
  ['lab/admin.json', admin],
];
for (const [slug, store] of Object.entries(STORES)) {
  files.push([`workflows/${store.folder}/skip-and-merge.json`, skipAndMerge(slug, store)]);
  files.push([`workflows/${store.folder}/stage-queues.json`, stageQueues(slug, store)]);
  for (const stage of STAGES) {
    files.push([`workflows/${store.folder}/stage-${stage.key}.json`, stageWorkflow(slug, store, stage)]);
    files.push([`workflows/${store.folder}/queue-stage-${stage.key}.json`, queueWorkflow(slug, store, stage)]);
  }
}

for (const folder of ['workflows', 'lab']) rmSync(join(root, folder), { recursive: true, force: true });
for (const [file, content] of files) {
  mkdirSync(dirname(join(root, file)), { recursive: true });
  writeFileSync(join(root, file), `${JSON.stringify(content, null, 2)}\n`);
}
console.log(`wrote ${files.length} workflows`);
