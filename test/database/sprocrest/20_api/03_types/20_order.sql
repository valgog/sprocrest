CREATE TYPE "order" AS (
    customer_number text,
    order_number text,
    items order_item[],
    status order_status,
    created timestamptz,
    shipped timestamptz,
    returned timestamptz
);
