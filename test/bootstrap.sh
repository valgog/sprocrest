#!/bin/bash

readonly owner="sproctest"
readonly databases="sproctest sproctest1 sproctest2"

readonly testdir="$( dirname $0 )"
cd "${testdir}"

function create_role() {
    local role=$1
    echo "CREATE role ${role} WITH LOGIN;"
}

function get_sources() {
    find database -type f -name '*.sql' | sort | xargs cat
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
    create_role $owner
    for db in $databases; do
        bootstrap_database $db
    done
}

get_bootstrap_script | psql -h localhost -U postgres -d postgres -f -
