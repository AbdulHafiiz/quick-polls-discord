PRAGMA foreign_keys = ON;
PRAGMA journal_mode = wal2;
PRAGMA synchronous = normal;
PRAGMA temp_store = memory;
PRAGMA mmap_size = 30000000000;
PRAGMA auto_vacuum = incremental;

BEGIN TRANSACTION;

    CREATE TABLE IF NOT EXISTS polls (
        id INTEGER PRIMARY KEY,
        "name" TEXT NOT NULL,
        creator_id INTEGER NOT NULL,
        question TEXT NOT NULL,
        choice_list TEXT NOT NULL,
        "status" TEXT NOT NULL DEFAULT 'active',
        created_at INTEGER NOT NULL,
        ended_at INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE IF NOT EXISTS votes (
        id INTEGER PRIMARY KEY,
        poll_id INTEGER NOT NULL,
        member_id INTEGER NOT NULL,
        vote_answer TEXT NOT NULL,
        voted_at INTEGER NOT NULL,
        FOREIGN KEY (poll_id) REFERENCES polls(id),
        UNIQUE(poll_id, member_id) ON CONFLICT ABORT
    ) STRICT;

COMMIT;