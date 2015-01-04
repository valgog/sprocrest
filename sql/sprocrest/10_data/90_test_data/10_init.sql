SET search_path TO test_data;

-- populate customer table

INSERT INTO test_data.customer (c_customer_number, c_first_name, c_last_name, c_email)
SELECT customer_number,
       first_name,
       last_name,
       format('%s.%s@example.org', lower(first_name), lower(last_name)) as email
  FROM (SELECT get_shard_id() || to_char(c.i, 'FM00000') as customer_number,
               ('{Florence,Helen,Forton,Martha,Jenatt,Adam,Bryan,Den,Hector,Walter}'::text[])[c.i] as first_name,
               ('{Bourn,Buck,Cay,Cramer,Toller,Wales,Wall,Watson,Yong,Wild}'::text[])[c.i] as last_name
          FROM generate_series(case get_shard_id() when '' then 1  when '1' then 1 when '2' then 6 end,
                               case get_shard_id() when '' then 10 when '1' then 5 when '2' then 10 end) as c(i)
       ) AS x;

-- populate order table
INSERT INTO test_data."order" (o_customer_id, o_order_number)
SELECT c_id,
       c_customer_number || to_char(o.i, 'FM000000') as order_number
  FROM test_data.customer,
       generate_series(1,5) as o(i);

-- populate order items
INSERT INTO test_data.order_item (oi_order_id, oi_sku, oi_description)
SELECT o_id,
       'SKU-' || ( select string_agg(chr((random() * 25)::int + 65),'') from generate_series(1,10) ) as random_sku,
       ('{Red,Green,Blue,White,Black}'::text[])[(random()*4)::int + 1] || ' ' || ('{pants,shirt,jeans,skirt,boot,belt}'::text[])[(random()*5)::int + 1] as description
  FROM test_data."order",
       generate_series(1,3) as s(i);
