SELECT stream, count(*) AS messages, min(ts) AS first, max(ts) AS last
FROM lake.agents.conversations
GROUP BY stream ORDER BY first;

SELECT stream, seq, role, left(content, 80) AS content, ts
FROM lake.agents.conversations
ORDER BY stream, seq;

SELECT stream AS run,
       count(*) FILTER (type = 'step') AS steps,
       string_agg(tools, ',') FILTER (tools IS NOT NULL) AS tools,
       max(total_tokens) AS total_tokens,
       min(ts) AS started, max(ts) AS ended
FROM lake.agents.agent_events
GROUP BY stream ORDER BY started;

SELECT time_bucket(INTERVAL '5 minutes', ts) AS bucket,
       count(*) FILTER (type = 'run_end') AS runs,
       sum(total_tokens) AS tokens
FROM lake.agents.agent_events
GROUP BY bucket ORDER BY bucket;
