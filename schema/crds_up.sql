CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    repo VARCHAR(255) NOT NULL,
    time TIMESTAMP NOT NULL,
    hash_sha1 BYTEA NOT NULL CHECK (octet_length(hash_sha1) = 20),
    alias_tag_id INTEGER REFERENCES tags(id),
    UNIQUE(name, repo),
    CHECK (alias_tag_id IS NULL OR alias_tag_id != id)
);

CREATE INDEX idx_tags_hash_sha1 ON tags(hash_sha1);

CREATE INDEX idx_tags_alias_tag_id ON tags(alias_tag_id);

CREATE TABLE crds (
    "group" VARCHAR(255) NOT NULL,
    version VARCHAR(255) NOT NULL,
    kind VARCHAR(255) NOT NULL,
    tag_id INTEGER NOT NULL REFERENCES tags (id) ON DELETE CASCADE,
    filename VARCHAR(1024) NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY(tag_id, "group", version, kind)
);

CREATE TABLE attempts (
    id SERIAL PRIMARY KEY,
    repo VARCHAR(255) NOT NULL,
    tag VARCHAR(255) NOT NULL,
    time TIMESTAMP NOT NULL,
    error VARCHAR(1000) NOT NULL,
    UNIQUE(repo, tag)
);

CREATE INDEX idx_attempts_time ON attempts(time);

