TODO fill this out

This tool assumes that all tables being replicated have a primary key column called `id`

When creating your schema, it's important to consider making sure the deduplication queries run quickly enough:

If you're not ordering your table by the event's gtid fields you'll want to add a projection that orders by them
so that the deduplication queries run quickly

ALTER TABLE mysql_bigcartel_binlog.products
    ADD PROJECTION prj_changelog_event_gtid
    (
        SELECT changelog_server_id, changelog_transaction_id, changelog_transaction_event_number
        ORDER BY (changelog_server_id, changelog_transaction_id, changelog_transaction_event_number)
    );

ALTER TABLE mysql_bigcartel_binlog.products
    materialize projection prj_changelog_gtid
```

If you're ordering not your table id you might want to add a projection that orders by id so dump deduplication queries run fast:
ALTER TABLE mysql_bigcartel_binlog.products
    ADD PROJECTION prj_id
    (
        SELECT id
        ORDER BY (id)
    );

ALTER TABLE mysql_bigcartel_binlog.products
    materialize projection prj_id;
```

In both cases, it's worth
