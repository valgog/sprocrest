CREATE FUNCTION get_orders(customer_number text, ignored int default 0)
RETURNS TABLE (orders "order")
AS $BODY$
BEGIN
    RETURN QUERY
    SELECT c_customer_number,
           o_order_number,
           ARRAY( SELECT ROW(oi_sku, oi_description)::order_item
                    FROM test_data.order_item
                   WHERE oi_order_id = o_id
                   ORDER BY oi_id ) as items,
           o_status,
           o_created,
           o_shipped,
           o_returned
      FROM test_data."order"
      JOIN test_data.customer ON c_id = o_customer_id
     WHERE c_customer_number = customer_number;
END;
$BODY$
LANGUAGE plpgsql
STABLE
STRICT;


CREATE FUNCTION get_orders(customer_number text, order_numbers text[])
RETURNS TABLE (orders "order")
AS $BODY$
BEGIN
    RETURN QUERY
    SELECT c_customer_number,
           o_order_number,
           ARRAY( SELECT ROW(oi_sku, oi_description)::order_item
                    FROM test_data.order_item
                   WHERE oi_order_id = o_id
                   ORDER BY oi_id ) as items,
           o_status,
           o_created,
           o_shipped,
           o_returned
      FROM test_data."order"
      JOIN test_data.customer ON c_id = o_customer_id
     WHERE c_customer_number = customer_number
       AND o_order_number = ANY( order_numbers );
END;
$BODY$
LANGUAGE plpgsql
STABLE
STRICT;
