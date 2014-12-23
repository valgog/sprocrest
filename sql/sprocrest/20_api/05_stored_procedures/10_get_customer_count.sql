CREATE FUNCTION get_customer_count()
RETURNS INT8
AS $BODY$
    SELECT count(c_customer_number)
      FROM test_data.customer
$BODY$
LANGUAGE sql
STABLE;

