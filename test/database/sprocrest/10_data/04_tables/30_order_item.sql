CREATE TABLE test_data.order_item (
    oi_id              serial primary key not null,
    oi_order_id        integer not null references test_data.order (o_id),
    oi_sku             text not null,
    oi_description     text null,
    oi_created         timestamptz not null default now(),
    oi_last_modified   timestamptz not null default now()
);
