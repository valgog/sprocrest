#!/bin/bash

readonly owner="sprocrest"
readonly databases="sprocrest sproctest1 sproctest2"


function drop_db() {
    local db=$1
    echo "DROP DATABASE ${db};"
}

function get_drop_script() {
    for db in $databases; do
        drop_db $db
    done
}

get_drop_script | psql -h localhost -U ${PGUSER:-postgres} -d postgres -f -
