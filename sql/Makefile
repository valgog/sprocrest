inject:
	find sprocrest -type f -name '*.sql' | sort | xargs cat | psql -d sprocrest -1 -f -

test:
	echo "set search_path to test_api, public; select to_json((f.*)) from get_orders('00001') as f;" | psql -d sprocrest -1 -f -
