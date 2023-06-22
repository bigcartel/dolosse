  ```
  ____   ___  _     ___  ____ ____  _____
 |  _ \ / _ \| |   / _ \/ ___/ ___|| ____|
 | | | | | | | |  | | | \___ \___ \|  _|
 | |_| | |_| | |__| |_| |___) |__) | |___
 |____/ \___/|_____\___/|____/____/|_____|
 ```

Single binary streaming change data capture from MySQL into ClickHouse.

# Features

- Replicating MySQL binlog change data to ClickHouse as separate rows with details about what columns were updated with each insert/update/delete event.
- No requirement for durable filesystem storage and no dependencies besides a MySQL and ClickHouse instance. Stores all replication state in ClickHouse.
- Only replicating the MySQL columns that are also present in the destination ClickHouse table and deduplicating row data. Any events which would result in a duplicate given the subset of columns in the destination table will be ignored.
- Deduplicating binlog data already written into ClickHouse such that you can rewind the syncer or re-run a database dump at any time without fear of writing duplicate data.
  If you add a new table in MySQL and later decide you want to add that table to ClickHouse You can add it and then restart dolosse with the
  --rewind flag and it will pull in all change data present in the binlog for that table while ignoring any data already present in ClickHouse for existing tables.
- Redacting sensitive user data using stable hashing to anonymize it (even nested within JSON/YAML columns) while still providing a means for querying that data in a meaningful way.
- Parsing columns containing YAML and converting them to JSON

# Schema/setup

You'll need to set binlog_format = 'ROW' in MySQL for Dolosse to work. To have the most seamless handling of changes in schema over time it's recommended to also set binlog_row_metadata = 'FULL'.
Dolosse will run fine with binlog_row_metadata = 'MINIMAL' but will fall back to assuming newly added columns are appended to the table, or skipping historical events on schema mismatch with --asume-only-append-columns=false.

Dolosse assumes that all tables being replicated have a primary key, but if they don't it will still work, it just won't deduplicate dump events.

given the following MySQL table in a database called "test"
```SQL
CREATE TABLE test (
        id int unsigned NOT NULL AUTO_INCREMENT,
        account_id int unsigned NOT NULL DEFAULT 0,
        label varchar(100),
        name varchar(100) NOT NULL DEFAULT '',
        price decimal(10,2) NOT NULL DEFAULT '0.00',
        visits int NOT NULL DEFAULT 0,
        description text,
        created_at datetime NOT NULL DEFAULT NOW()
    );

INSERT INTO test (id, name, label, price, price_two, description)
    VALUES (
            1,
            "test thing",
            "label",
            "12.31",
            "my cool description"
           );
UPDATE test set name="something else";
```

You can create a destination table in ClickHouse in a schema we'll call "test" with a subset of columns along with the columns Dolosse requires for replication
```
CREATE DATABASE IF NOT EXISTS test_mysql_changelog;
CREATE TABLE test_mysql_changelog.test (
        id Int32,
        name String,
        price Decimal(10, 2),
        description Nullable(String),
        created_at DateTime,
        changelog_action LowCardinality(String),
        changelog_event_created_at DateTime64(9),
        changelog_gtid_server_id LowCardinality(String),
        changelog_gtid_transaction_id UInt64,
        changelog_gtid_transaction_event_number UInt32,
        changelog_updated_columns Array(LowCardinality(String))
    )
ENGINE = MergeTree
ORDER BY (id, changelog_event_created_at);

ALTER TABLE test_mysql_changelog.test
    ADD PROJECTION prj_changelog_event_gtid
    (
        SELECT changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number
        ORDER BY (changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number)
    );
```

Then you can start dolosse (assuming everything is running on default ports on localhost) with `dolosse --mysql-db="test" --clickhouse-db="test_mysql_changelog"`.

TODO: show examples of inserts/updates/deletes and how they appear in ClickHouse after they're done.

Dolosse stores all of it's state in ClickHouse using the EmbeddedRocksDB database engine in a table called binlog_sync_state that
will be created in the database you specify with --clickhouse-db. The motivation being that dolosse has no additional dependencies
for state storage and can be run in ephemeral environments like kubernetes without concern about persistent local storage.

# Performance

When creating your schema, it's important to consider making sure the deduplication queries run quickly enough:

If you're not ordering your table by the event's gtid fields you'll want to add a projection that orders by them
so that the deduplication queries run quickly

```
ALTER TABLE mysql_changelog.products
    ADD PROJECTION prj_changelog_event_gtid
    (
        SELECT changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number
        ORDER BY (changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number)
    );

ALTER TABLE mysql_changelog.products
    materialize projection prj_changelog_gtid
```

If you're not ordering your table by id you might want to add a projection that orders by id so dump deduplication queries run fast:

```
ALTER TABLE mysql_changelog.products
    ADD PROJECTION prj_id
    (
        SELECT id
        ORDER BY (id)
    );

ALTER TABLE test_mysql_binlog.products
    materialize projection prj_id;
```

In both cases, it's worth checking `select query_duration_ms, query from system.query_log order by event_time desc limit 30;` while
Dolosse is running to see if the slowness of queries warrents the addition of these projections since they use disk space.

# TODO

- Fix error noise when using --help flag
- Bolster documentation
    - Add specific example of a source MySQL table and destination ClickHouse table plus a start up command
    - Add common use case examples to readme
    - Include basic example config in readme
- Integrate tool for ClickHouse schema suggestion based on MySQL source table
- Add github builds & releases for Arm/X86
- Nice (bubbletea)[https://github.com/charmbracelet/bubbletea] TUI that shows replication stats for each table and allows for some runtime config
