DATABASES = sprocrest sprocrests1 sprocrests2

inject:
	$(foreach db, $(DATABASES), \
	  find sprocrest -type f -name '*.sql' | sort | xargs cat | psql -d $(db) -1 -f - ; \
	)

test:
	$(foreach db, $(DATABASES), \
	  echo "set search_path to test_api, public; select to_json((f.*)) from get_orders('00001') as f;" | psql -d $(db) -1 -f - ; \
	)

all: inject
