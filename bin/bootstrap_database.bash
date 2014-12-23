#!/bin/bash

readonly owner="sprocrest"
readonly databases="sprocrest sproctest1 sproctest2"

readonly bindir="$( dirname $0 )"
cd "${bindir}/../sql"

PSQL="psql -v ON_ERROR_STOP=on -tA -h localhost -U ${PGUSER:-postgres} -d postgres -f -"

function cleanup_cluster() {
    (
    $PSQL << --SQL--
\\connect postgres
SELECT 'DROP DATABASE ' || quote_ident(datname) || ';' FROM pg_database where datdba > 10;
SELECT 'DROP ROLE ' || quote_ident(rolname) || ';' FROM pg_roles where OID > 10;
--SQL--
    ) | grep 'DROP'
}

function create_role() {
    local role=$1
    echo "CREATE role ${role} WITH LOGIN;"
}

function get_sources() {
    find . -type f -name '*.sql' | sort | xargs cat
}

function bootstrap_database() {
    local db=$1
    cat << EOF
RESET role;
CREATE DATABASE ${db} owner ${owner};
\connect ${db}
SET role TO ${owner};
EOF
    get_sources
}

function get_bootstrap_script() {
    cleanup_cluster
    create_role $owner
    for db in $databases; do
        bootstrap_database $db
    done
}

get_bootstrap_script | $PSQL
