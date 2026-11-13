-- The table of the thread in both likely shapes. `status` is a set of tags;
-- a stage adds its tag to a row when it has finished that row.
CREATE TABLE events (
  id integer PRIMARY KEY,
  title text NOT NULL,
  status text[] NOT NULL DEFAULT ARRAY['candidate']
);

CREATE TABLE events_jsonb (
  id integer PRIMARY KEY,
  title text NOT NULL,
  status jsonb NOT NULL DEFAULT '["candidate"]'
);

INSERT INTO events (id, title) SELECT n, 'Event ' || n FROM generate_series(1, 137) AS n;
INSERT INTO events_jsonb (id, title) SELECT n, 'Event ' || n FROM generate_series(1, 137) AS n;
