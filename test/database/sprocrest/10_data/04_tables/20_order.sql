CREATE TABLE test_data."order" (
    o_id               serial primary key not null,
    o_customer_id      integer not null references test_data.customer (c_id),
    o_order_number     text not null unique,
    o_status           test_data.order_status not null default 'INITIAL',
    o_created          timestamptz not null default now(),
    o_last_modified    timestamptz not null default now(),
    o_shipped          timestamptz null,
    o_returned         timestamptz null
);
