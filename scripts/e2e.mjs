// npm run test:e2e
// Starts the test stack, runs test/e2e.test.mjs against it and removes the
// stack again. Set KEEP_STACK=1 to leave it running, for example to open the
// n8n editor on http://localhost:55693 and look at the executions.

import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const run = (command, args) => spawnSync(command, args, { cwd: root, stdio: 'inherit' }).status ?? 1;

run('sh', ['test/stack.sh', 'down']);
let status = run('sh', ['test/stack.sh', 'up']);
if (status === 0) status = run(process.execPath, ['--test', '--test-concurrency=1', '--test-reporter=spec', 'test/e2e.test.mjs']);
if (status !== 0) run('sh', ['test/stack.sh', 'logs']);
if (!process.env.KEEP_STACK) run('sh', ['test/stack.sh', 'down']);
process.exit(status);
