TODO fill this out

This tool assumes that all tables being replicated have a primary key column called `id`

When creating your schema, it's important to consider making sure the deduplication queries run quickly enough:

If you're ordering your table by id you'll want to add a projection that orders by changelog_event_created_at and includes changelog_id
so that the deduplication queries run quickly
```
ALTER TABLE mysql_bigcartel_binlog.products
    ADD PROJECTION prj_changelog_event_created_at
    (
        SELECT changelog_id, changelog_event_created_at
        ORDER BY (changelog_event_created_at)
    );

ALTER TABLE mysql_bigcartel_binlog.products
    materialize projection prj_changelog_event_created_at;
```

If you're ordering your table by changelog_event_created_at you might want to add a projection that orders by id so dump deduplication queries run fast:
```
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
