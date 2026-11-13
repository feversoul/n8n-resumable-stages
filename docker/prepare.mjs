// Used by bootstrap.sh inside the n8n container: writes the import files for
// the n8n CLI. The Postgres credential is made from the environment, so no
// password is stored in the repository.
//   node prepare.mjs <output dir> <workflows dir>...

import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [out, ...sources] = process.argv.slice(2);
const env = process.env;

const credentials = [
  {
    id: 'stagesPostgres',
    name: 'Stages Postgres',
    type: 'postgres',
    data: {
      host: env.STAGES_DB_HOST || 'postgres',
      port: Number(env.STAGES_DB_PORT || 5432),
      database: env.STAGES_DB_NAME || 'stages',
      user: env.STAGES_DB_USER || 'stages',
      password: env.STAGES_DB_PASSWORD,
      ssl: 'disable',
    },
  },
];

mkdirSync(join(out, 'workflows'), { recursive: true });
writeFileSync(join(out, 'credentials.json'), JSON.stringify(credentials));

// n8n refuses to publish a workflow whose sub-workflows are not published yet,
// so the publish order lists the sub-workflows first.
const first = [];
const last = [];
for (const source of sources) {
  for (const file of readdirSync(source).filter((name) => name.endsWith('.json'))) {
    const workflow = JSON.parse(readFileSync(join(source, file), 'utf8'));
    writeFileSync(join(out, 'workflows', `${workflow.id}.json`), JSON.stringify(workflow));
    const calls = workflow.nodes.some((node) => node.type === 'n8n-nodes-base.executeWorkflow');
    (calls ? last : first).push(workflow.id);
  }
}
writeFileSync(join(out, 'publish-order.txt'), `${[...first, ...last].join('\n')}\n`);
