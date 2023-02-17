TODO fill this out

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
