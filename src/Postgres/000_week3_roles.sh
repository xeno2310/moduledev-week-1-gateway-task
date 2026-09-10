#!/bin/sh
set -eu

: "${COURSE_MIGRATOR_PASSWORD:?COURSE_MIGRATOR_PASSWORD is required}"
: "${COURSE_PUBLISHER_PASSWORD:?COURSE_PUBLISHER_PASSWORD is required}"
: "${COURSE_RUNTIME_PASSWORD:?COURSE_RUNTIME_PASSWORD is required}"
: "${COURSE_WORKER_PASSWORD:?COURSE_WORKER_PASSWORD is required}"
: "${COURSE_OUTBOX_PASSWORD:?COURSE_OUTBOX_PASSWORD is required}"
: "${COURSE_INBOX_PASSWORD:?COURSE_INBOX_PASSWORD is required}"

psql \
    -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=migrator_password="$COURSE_MIGRATOR_PASSWORD" \
    --set=publisher_password="$COURSE_PUBLISHER_PASSWORD" \
    --set=runtime_password="$COURSE_RUNTIME_PASSWORD" \
    --set=worker_password="$COURSE_WORKER_PASSWORD" \
    --set=outbox_password="$COURSE_OUTBOX_PASSWORD" \
    --set=inbox_password="$COURSE_INBOX_PASSWORD" <<'SQL'

DO $roles$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'course_owner'
    ) THEN
        CREATE ROLE course_owner NOLOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'course_migrator'
    ) THEN
        CREATE ROLE course_migrator LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'course_publication'
    ) THEN
        CREATE ROLE course_publication LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'course_runtime'
    ) THEN
        CREATE ROLE course_runtime LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'workflow_worker'
    ) THEN
        CREATE ROLE workflow_worker LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'outbox_dispatcher'
    ) THEN
        CREATE ROLE outbox_dispatcher LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'inbox_reconciler'
    ) THEN
        CREATE ROLE inbox_reconciler LOGIN;
    END IF;
END
$roles$;

ALTER ROLE course_migrator
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'migrator_password';

ALTER ROLE course_publication
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'publisher_password';

ALTER ROLE course_runtime
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'runtime_password';

ALTER ROLE workflow_worker
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'worker_password';

ALTER ROLE outbox_dispatcher
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'outbox_password';

ALTER ROLE inbox_reconciler
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS
    PASSWORD :'inbox_password';

GRANT course_owner TO course_migrator;

GRANT USAGE, CREATE
ON SCHEMA public
TO course_migrator;

-- PostgreSQL grants EXECUTE on newly created functions to
-- PUBLIC by default.  Remove that global default so bootstrap
-- functions such as pgcrypto are not executable by the Python
-- perimeter roles.
--
-- course_owner is the trusted application owner and explicitly
-- receives EXECUTE on functions created by the bootstrap role.
ALTER DEFAULT PRIVILEGES
REVOKE EXECUTE ON FUNCTIONS
FROM PUBLIC;

ALTER DEFAULT PRIVILEGES
GRANT EXECUTE ON FUNCTIONS
TO course_owner;

SQL
