inject:
	find sprocrest -type f -name '*.sql' | sort | xargs cat | psql -d sprocrest -1 -f -

test:
	echo "set search_path to test_api, public; select * from get_orders('00000001');" | psql -d sprocrest -1 -f -
