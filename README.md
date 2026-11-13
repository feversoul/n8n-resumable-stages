# n8n resumable stages

[![test](https://github.com/feversoul/n8n-resumable-stages/actions/workflows/test.yml/badge.svg)](https://github.com/feversoul/n8n-resumable-stages/actions/workflows/test.yml)

A Postgres table holds rows with a `status` column that is a set of tags. Each stage of a pipeline is a sub-workflow that adds its tag to a row when that row is done, so a run that was interrupted can go on later without redoing finished stages. This repo rebuilds the graph from the forum thread ["Data stream handling for sub-workflow"](https://community.n8n.io/t/325555), shows why a stage sub-workflow runs twice after an interruption, and ships two structures that run every stage once and still skip what is already tagged.

Everything here was run on a real n8n 2.47.7 with PostgreSQL 17 and 137 rows. The three stages (`Vectorize News Record`, `Duplicate Finder`, `Event Relevancy Screening`) are stand-ins: they call a small counting service once per row and write the tag. No real embedding, duplicate search or model call was made.

| Asked for                                              | What you get                                                                                                                                                                                                     |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Why does `Event Relevancy Screening` run twice?        | Reproduced with the same numbers: after an interruption in Duplicate Finder at row 123, the next run calls Duplicate Finder with 14 rows and Screening with 137 and with 123. Those 123 rows are screened twice. |
| Keep "skip what is already tagged"                     | Both structures skip per row and per stage. A second run after an interruption calls each stage once, with exactly the rows that lack its tag (for example 14, then 137).                                        |
| Few node pathways, not a linear redo-everything flow   | **Stage queues**: the parent is three sub-workflow nodes in a row, no Select, no Switch, no wire back. **Skip and merge**: one IF and one Merge per stage, sub-workflows unchanged.                              |
| Is "IF, then both wires into the next IF" good enough? | No row is processed twice, but the next stage still runs twice (14 + 123). A Merge between the stages makes it one call.                                                                                         |
| Is "one Select per stage in a row" good enough?        | Not for the resume. The first Select that finds no rows returns no items and nothing after it runs: 14 rows stayed without the duplicate tag and 137 without screening.                                          |
| `status` as `text[]` or as `jsonb`                     | Both, in `workflows/text-array/` and `workflows/jsonb/`. The IF conditions are the same; only the SQL differs.                                                                                                   |

## What was measured

Each structure was run against 137 fresh rows: once clean, and three times with an interruption (the stage's work call fails) followed by a second run. The stand-in records every sub-workflow call and every row. "137+123" means two calls in one run.

Second run after an interruption in **Duplicate Finder at row 123** (the 14 / 123 split of the thread's screenshot):

| Structure                           | Vectorize  | Duplicate Finder | Relevancy Screening | Rows a stage worked on twice | Rows left behind                |
| ----------------------------------- | ---------- | ---------------- | ------------------- | ---------------------------- | ------------------------------- |
| Loop back (the graph of the thread) | not called | 14               | **137 + 123**       | **123 in Screening**         | none                            |
| IF chain without Merge              | not called | 14               | **14 + 123**        | none                         | none                            |
| Select chain (one Select per stage) | not called | not called       | not called          | none                         | **14 duplicate, 137 screening** |
| **Skip and merge**                  | not called | 14               | 137                 | none                         | none                            |
| **Stage queues**                    | 0 rows     | 14               | 137                 | none                         | none                            |

Second run after an interruption in **Vectorize at row 50**:

| Structure              | Vectorize | Duplicate Finder | Relevancy Screening | Rows a stage worked on twice | Rows left behind |
| ---------------------- | --------- | ---------------- | ------------------- | ---------------------------- | ---------------- |
| Loop back              | 87        | **137 + 50**     | 137                 | **50 in Duplicate Finder**   | none             |
| IF chain without Merge | 87        | **87 + 50**      | **87 + 50**         | none                         | none             |
| Select chain           | 87        | 137              | 137                 | none                         | none             |
| **Skip and merge**     | 87        | 137              | 137                 | none                         | none             |
| **Stage queues**       | 87        | 137              | 137                 | none                         | none             |

Second run after an interruption in **Relevancy Screening at row 60**: every structure calls Screening once with the 77 rows that are left, except the Select chain, which calls nothing and leaves those 77 rows behind.

Clean run: every structure calls each stage once with 137 rows. A further run with nothing left to do calls no stage (Stage queues: three calls with 0 rows).

**n8n killed for real.** The n8n container was killed (`docker kill`) while Duplicate Finder was at about row 30, then started again. n8n did not continue the dead run by itself. The next run of Skip and merge and of Stage queues called Duplicate Finder once with the untagged rows (102 and 103 in the last run) and Screening once with 137. One row was worked on twice: the row that was in flight at the kill, whose work had reached the stand-in but whose tag was not saved yet.

All of the above is the same for `text[]` and `jsonb` (the two shipped structures were run on both; the three lab structures on `text[]` only).

## Why the stage runs twice

The graph of the thread is `Select rows -> Switch -> one sub-workflow per missing tag`, with every sub-workflow wired back into `Select rows` (`lab/loop-back.json`). On a clean run all rows are at the same stage, so only one Switch output has rows at a time and each stage runs once.

After an interruption the rows are at two stages. The Switch sends 14 rows to Duplicate Finder and 123 to Screening in the same pass. Duplicate Finder finishes first, its wire back reads the table again, and now all 137 rows lack the screening tag, so the Switch sends 137 to Screening while the 123 from the first pass are still waiting. Screening then runs for both. The tags are not the cause; the two wires back are.

In this lab the call with 137 rows came before the call with 123. The thread reports them the other way round. The counts are the same.

## The two structures

### Stage queues (fewest nodes)

`Trigger -> Vectorize News Record -> Duplicate Finder -> Event Relevancy Screening`

Each sub-workflow starts with its own query: rows that are ready for it and do not carry its tag yet. For Duplicate Finder on `text[]`:

```sql
SELECT id, title, status
FROM events
WHERE status && ARRAY['vectorize']
  AND NOT status && ARRAY['duplicate', 'unique']
ORDER BY id
```

On `jsonb` the two conditions are `status ?| ARRAY['vectorize']` and `NOT status ?| ARRAY['duplicate', 'unique']`.

Three settings make it work:

- The query node has **Always Output Data** on, and the sub-workflow ends in a node that returns one item in every case. Otherwise a stage with nothing to do returns no items and the stages after it never start.
- Each Execute Sub-workflow node in the parent has **Execute Once** on.
- The sub-workflow writes the tag row by row, right after the row's work, not once at the end.

The skipping lives in the query, so the parent needs no Select, no Switch and no IF.

### Skip and merge (sub-workflows stay as they are)

`Trigger -> Select rows -> [IF needs stage -> sub-workflow] -> Merge -> [IF -> sub-workflow] -> Merge -> ...`

The table is read once. Per stage, an IF sends the rows that lack the tag into the sub-workflow and the others around it, and a Merge (Append) joins both before the next IF. The Merge is what turns two arrivals into one call. The IF condition for Duplicate Finder:

```
{{ !$json.status.some((tag) => ['duplicate', 'unique'].includes(tag)) }}
```

The sub-workflow must return the rows with their new `status`, because the next IF looks at it. The stand-ins do that with `UPDATE ... RETURNING`.

### Which one

Stage queues, if you can add one query node to the front of each sub-workflow. The parent becomes three nodes, there is nothing to wire wrongly, each stage always reads the current table, and it does not matter what a sub-workflow returns. Skip and merge is the choice when the sub-workflows cannot be changed or should keep taking their rows from the parent; it costs an IF and a Merge per stage.

Both hand a stage only the rows it has not tagged. If Duplicate Finder compares a row with the other rows of the same call, it has to compare with the table instead: on a resume the call holds only the leftover rows (14 here). That is already true in the original graph.

## Add it to your n8n

Works on any n8n that can reach your Postgres, n8n Cloud included.

1. Import the files from `workflows/text-array/` or `workflows/jsonb/`, depending on the type of your `status` column (`\d your_table` in psql shows it).
2. Create a Postgres credential and select it in the Postgres nodes.
3. Open each Execute Sub-workflow node and pick the sub-workflow again; an import through the editor gives workflows new ids.
4. Publish the sub-workflows first, then the parent. n8n 2.47.7 refuses to publish a parent whose sub-workflows are not published.
5. Replace the table and column names in the SQL, the tags in the IF conditions and queries, and the Webhook with your own trigger.
6. Replace `Do the work` in each stand-in stage with your real stage, or copy only the structure into your own workflows: the first query node for Stage queues, the IF and Merge for Skip and merge.

To try it locally instead: `docker compose up -d`, then open http://localhost:5678. The stack brings a Postgres with both tables (137 rows each) and the stand-in. Start a run with `curl -X POST http://localhost:5678/webhook/stages/text/stage-queues` and read the counts from `http://localhost:8080/report`.

## What was tested

`npm run test:e2e` starts n8n 2.47.7, PostgreSQL 17 and the stand-in in Docker and runs 30 tests over the real webhooks.

| Test                                                                                                     | Result                                                                                   |
| -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Loop back: clean run, and a second run after an interruption in each of the 3 stages                     | pass, numbers as in the tables above (a stage runs twice, rows are worked on twice)      |
| IF chain without Merge: the same 4 scenarios                                                             | pass, two calls with different rows                                                      |
| Select chain: the same 4 scenarios                                                                       | pass, rows left behind after an interruption in stage 2 or 3                             |
| Skip and merge on `text[]` and on `jsonb`: 4 scenarios each                                              | pass: no stage called twice in a run, no row worked on twice, no row left behind         |
| Stage queues on `text[]` and on `jsonb`: 4 scenarios each                                                | pass: the same                                                                           |
| Skip and merge, Stage queues: n8n container killed in the middle of Duplicate Finder, started, run again | pass: each stage once, no row left behind, at most the one in-flight row worked on twice |

The numbers are pinned in `test/e2e.test.mjs`; a different n8n version that behaves differently fails the test.

## Not tested, and other limits

- Real stages. The stand-ins do one HTTP call per row and tag the row; a real embedding, duplicate search or model call was not made.
- Two runs at the same time. Nothing here claims a row before working on it, so two overlapping executions would both pick the same untagged rows. Keep one run at a time, or add a claim step.
- A row whose work was done but whose tag was not saved is done again on the next run (measured: one row after the kill). If a stage must not repeat, it needs its own idempotency.
- The stand-in stage stops at the first failing row. Stages that continue past a failed row were not tested.
- A sub-workflow that tags all rows at its end instead of row by row loses the whole stage's progress on an interruption.
- The original workflow was read from a screenshot. `Select rows` is assumed to return the whole table once per run (Execute Once).
- `varchar[]` columns (the `ARRAY['...']` literals may need a cast), queue mode, n8n Cloud, tables larger than 137 rows, Execute Sub-workflow in "run once for each item" mode.

## Files

- `workflows/text-array/`, `workflows/jsonb/`: `skip-and-merge.json`, `stage-queues.json` and the stand-in stages they call (`stage-*.json` take rows from the parent, `queue-stage-*.json` read their own work list).
- `lab/`: `loop-back.json`, `if-chain.json`, `select-chain.json` (the structures measured against them) and `admin.json` (seeds and reads the table for the tests).
- `scripts/build.mjs` generates all workflow files; `docker/standin.mjs` is the counting stand-in; `docker/init.sql` creates the tables.

MIT licensed.
