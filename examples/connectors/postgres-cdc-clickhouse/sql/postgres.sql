CREATE TABLE merchants (
    id text PRIMARY KEY,
    name text NOT NULL,
    region text NOT NULL
);

CREATE TABLE order_events (
    id serial PRIMARY KEY,
    order_id text NOT NULL,
    region text NOT NULL,
    merchant_id text NOT NULL REFERENCES merchants (id),
    "type" text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE order_events REPLICA IDENTITY FULL;

INSERT INTO merchants (id, name, region) VALUES
    ('lemongrass', 'Lemongrass', 'eu'),
    ('brasserie', 'Brasserie', 'eu'),
    ('diner', 'Night Diner', 'na'),
    ('taco', 'Taco Window', 'na'),
    ('ramen', 'Late Ramen', 'apac'),
    ('hawker', 'Hawker 12', 'apac');
