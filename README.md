  ____   ___  _     ___  ____ ____  _____
 |  _ \ / _ \| |   / _ \/ ___/ ___|| ____|
 | | | | | | | |  | | | \___ \___ \|  _|
 | |_| | |_| | |__| |_| |___) |__) | |___
 |____/ \___/|_____\___/|____/____/|_____|

Simple and efficient mysql binlog to clickhouse replication.

Dolosse supports:
- Parsing columns containing YAML and converting them to JSON
- Stably hashing fields of sensitive user data to anonymize it (even nested within YAML columns) while still providing a means for querying that data.
- Only replicating the mysql columns that are also present in the destination clickhouse table and deduplicating row data. Any events which would result in a duplicate given the subset of columns in the destination table will be ignored.
- Pulling the maximum amount of binlog data available, and dumping only data not already present in the binlog.
- Deduplicating binlog data such that you can rewind the syncer or re-run a database dump at any time without fear of writing duplicate data.

You'll need to set binlog_format = 'ROW' for Dolosse to work.
To have the most seamless handling of changes in schema over time it's recommended to also set binlog_row_metadata = 'FULL'.
Dolosse will run fine with binlog_row_metadata = 'MINIMAL' but will fall back to assuming newly added columns are appended
to the table, or skipping historical events on schema mismatch with --asume-only-append-columns=false.

This tool assumes that all tables being replicated have a primary key, but if they don't it will still work, it just won't deduplicate dump events.

When creating your schema, it's important to consider making sure the deduplication queries run quickly enough:

If you're not ordering your table by the event's gtid fields you'll want to add a projection that orders by them
so that the deduplication queries run quickly

```
ALTER TABLE mysql_bigcartel_binlog.products
    ADD PROJECTION prj_changelog_event_gtid
    (
        SELECT changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number
        ORDER BY (changelog_gtid_server_id, changelog_gtid_transaction_id, changelog_gtid_transaction_event_number)
    );

ALTER TABLE mysql_bigcartel_binlog.products
    materialize projection prj_changelog_gtid
```

If you're not ordering your table by id you might want to add a projection that orders by id so dump deduplication queries run fast:

```
ALTER TABLE mysql_bigcartel_binlog.products
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
