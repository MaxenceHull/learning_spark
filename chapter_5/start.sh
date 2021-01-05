#!/bin/bash

function usage() {
    cat <<HELP
Usage:
    $0 <command> [...]
    $0 [-h|--help]
    Commands:
    start_dependencie: build psql image and start container
    stop_dependencie: stop and remove psql container
    run_app: start spark application
    run: start dependencies, run application and stop dependencies
    clean: remove docker images and useless directories

HELP
}

function die() {
    local msg="$@"
    echo
    echo "FAILURE: ${msg}" >&2
    exit 1
}

function start_dependencie() {
    docker build -t test-spark .
    docker run -d -p 5432:5432 --name spark-db -e POSTGRES_PASSWORD=pwd test-spark
}

function stop_dependencie(){
    docker stop spark-db
    docker rm spark-db
}

function run_app(){
    export PYTHONIOENCODING=utf8
    # Remove spark temp files
    rm -r spark-warehouse
    spark-submit --driver-class-path postgresql-42.2.18.jre6.jar --jars postgresql-42.2.18.jre6.jar jdbc.py
}

function clean(){
    rm -r spark-warehouse
    docker image rm test-spark
    docker system prune
}


# expect at least 1 argument
[ "$#" -lt 1 ] && usage && die "No command specified."

for cmd in "$@"; do
    case "${cmd}" in
        (start_dependencie)
            start_dependencie || die "Cannot start postgres container"
            ;;

        (stop_dependencie)
            stop_dependencie || die "Cannot stop or remove postgres container"
            ;;
        
        (run_app)
            run_app || die "Cannot run spark application"
            ;;

        (run)
            start_dependencie || die "Cannot start postgres container"
            run_app || die "Cannot run spark application"
            stop_dependencie || die "Cannot stop or remove postgres container"
            ;;

        (clean)
            clean || die "Cannot clean directory"
            ;;

        (-h|--help)
            usage
            ;;

        (*)
            usage
            die "Unknown command '${cmd}'."
            ;;
    esac
done