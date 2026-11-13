// The far side of the stand-in stages: it counts what actually arrives.
// Nothing is forwarded anywhere, everything lives in memory.
//
//   POST /reset  { failStage, failAfter, delayMs, keep }
//                 Forget everything (unless keep is true). Optional: answer 500
//                 to one work call of `failStage`, after `failAfter` of its rows
//                 went through (the interruption), and hold every work call for
//                 `delayMs`.
//   POST /call   { stage, ids }   one per run of a stage sub-workflow
//   POST /work   { stage, id }    one per row; answers the tag the stage gives it
//   GET  /report                  { calls: [{ stage, items, ids }], work: [{ stage, id, ok }] }
//   GET  /health

import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';

let plan = {};
let fired = false;
let calls = [];
let work = [];

// What each stage decides for a row. Fixed per id, so a repeat gives the same tag.
const TAG = {
  vectorize: () => 'vectorize',
  duplicate: (id) => (id % 7 === 0 ? 'duplicate' : 'unique'),
  relevancy: (id) => (id % 3 === 0 ? 'irrelevant' : 'relevant'),
};

const send = (res, status, body) => {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
};
const read = (req) =>
  new Promise((resolve) => {
    let text = '';
    req.on('data', (chunk) => (text += chunk));
    req.on('end', () => {
      try {
        resolve(JSON.parse(text || '{}'));
      } catch {
        resolve({});
      }
    });
  });

createServer(async (req, res) => {
  const { pathname } = new URL(req.url, 'http://standin');
  if (req.method === 'GET' && pathname === '/health') return send(res, 200, { ok: true });
  if (req.method === 'GET' && pathname === '/report') return send(res, 200, { calls, work });

  const body = await read(req);
  if (req.method === 'POST' && pathname === '/reset') {
    plan = body;
    fired = false;
    if (!body.keep) {
      calls = [];
      work = [];
    }
    return send(res, 200, { ok: true });
  }
  if (req.method === 'POST' && pathname === '/call') {
    const ids = Array.isArray(body.ids) ? body.ids : [];
    calls.push({ stage: body.stage, items: ids.length, ids });
    return send(res, 200, { ok: true, call: calls.length });
  }
  if (req.method === 'POST' && pathname === '/work') {
    const { stage, id } = body;
    if (!TAG[stage] || !Number.isInteger(id)) return send(res, 400, { error: 'stage and a whole-number id are required' });
    const done = work.filter((entry) => entry.stage === stage && entry.ok).length;
    if (!fired && plan.failStage === stage && done >= Number(plan.failAfter ?? 0)) {
      fired = true;
      work.push({ stage, id, ok: false });
      return send(res, 500, { error: `interrupted in ${stage} at row ${id}` });
    }
    // Counted when it arrives: the far side has seen the row, whatever happens next.
    work.push({ stage, id, ok: true });
    if (plan.delayMs) await sleep(Number(plan.delayMs));
    return send(res, 200, { id, tag: TAG[stage](id) });
  }
  send(res, 404, { error: 'not found' });
}).listen(8080, () => console.log('stand-in listening on 8080'));
