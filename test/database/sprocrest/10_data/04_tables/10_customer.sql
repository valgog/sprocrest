CREATE TABLE test_data.customer (
    c_id               serial primary key not null,
    c_customer_number  text not null unique,
    c_first_name       text not null,
    c_last_name        text not null,
    c_email            text not null check (btrim(c_email) = c_email and length(c_email)>3),
    c_is_active        boolean not null default true,
    c_created          timestamptz not null default now(),
    c_last_modified    timestamptz not null default now()
);

CREATE UNIQUE INDEX ON test_data.customer (lower(c_email));

