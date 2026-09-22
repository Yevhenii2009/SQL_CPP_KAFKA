CREATE ROLE debezium
WITH LOGIN
REPLICATION
PASSWORD 'dbz_password';

GRANT CONNECT ON DATABASE demo TO debezium;

GRANT USAGE ON SCHEMA public TO debezium;

CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

GRANT SELECT ON TABLE public.orders TO debezium;

GRANT USAGE, SELECT ON SEQUENCE public.orders_id_seq TO debezium;

CREATE PUBLICATION debezium_publication
FOR TABLE public.orders;