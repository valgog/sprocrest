CREATE FUNCTION get_orders(customer_number text)
RETURNS TABLE (orders "order")
AS $BODY$
DECLARE
    known_customer_number CONSTANT text := '00000001';
BEGIN

    IF customer_number != known_customer_number THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT known_customer_number,
           to_char(o_id, 'FM000000000'),
           ARRAY(select ROW('SKU-' || to_char(i_id, 'FM00000'), 'Article whithout description' )::order_item
                   from generate_series(1,5) as i(i_id)
                )::order_item[],
           'INITIAL'::order_status,
           'today'::timestamptz,
           NULL::timestamptz,
           NULL::timestamptz
      FROM generate_series(1,10) AS o(o_id);
END;
$BODY$
LANGUAGE plpgsql
STABLE
STRICT;

