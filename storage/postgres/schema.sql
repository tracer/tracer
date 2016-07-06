CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION duration(d tstzrange) RETURNS integer
       AS 'SELECT (EXTRACT(epoch from upper($1) - lower($1)) * 1e9)::int'
       LANGUAGE SQL
       IMMUTABLE
       RETURNS NULL ON NULL INPUT;

CREATE TABLE spans (
       id bigint PRIMARY KEY,
       trace_id bigint,
       time tstzrange NOT NULL,
       service_name text NOT NULL,
       operation_name text NOT NULL
);

CREATE INDEX idx_spans_trace_id ON spans (trace_id);
CREATE INDEX idx_spans_time ON spans USING gist (time);
CREATE INDEX idx_spans_operation_name ON spans (operation_name);

CREATE TABLE tags (
       id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
       trace_id bigint NOT NULL REFERENCES spans ON DELETE CASCADE,
       span_id bigint NOT NULL REFERENCES spans ON DELETE CASCADE,
       key text NOT NULL,
       value text NOT NULL,
       time timestamp with time zone NULL
);

CREATE INDEX idx_tags_span_id ON tags (span_id);
CREATE INDEX idx_tags_key_value ON tags (key, value);

CREATE TYPE relation AS ENUM ('parent');

CREATE TABLE relations (
       id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
       span1_id bigint NOT NULL REFERENCES spans ON DELETE CASCADE,
       span2_id bigint NOT NULL REFERENCES spans ON DELETE CASCADE,
       kind relation NOT NULL
);

CREATE INDEX idx_relations_span1_id ON relations (span1_id);
CREATE INDEX idx_relations_span2_id ON relations (span2_id);
