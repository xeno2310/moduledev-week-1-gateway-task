#!/bin/sh
set -eu

: "${COURSE_AUTOCHECK_PASSWORD:?COURSE_AUTOCHECK_PASSWORD is required}"

# Privileged administrative bootstrap only. Existing volumes require an
# explicit administrator invocation; application services never create roles.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv reader_password COURSE_AUTOCHECK_PASSWORD
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'autocheck_reader') THEN
        CREATE ROLE autocheck_reader LOGIN;
    END IF;
END
$roles$;

ALTER ROLE autocheck_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS PASSWORD :'reader_password';

DO $memberships$
DECLARE
    membership record;
BEGIN
    FOR membership IN
        SELECT r.rolname FROM pg_catalog.pg_auth_members AS m
        JOIN pg_catalog.pg_roles AS r ON r.oid = m.roleid
        WHERE m.member = 'autocheck_reader'::regrole
    LOOP
        EXECUTE format('REVOKE %I FROM autocheck_reader', membership.rolname);
    END LOOP;
END
$memberships$;

-- Production boundaries use no temporary relations. A direct reader REVOKE
-- cannot deny TEMP inherited from PUBLIC. Existing explicit grants survive.
REVOKE TEMP ON DATABASE course FROM PUBLIC;
REVOKE ALL ON DATABASE course FROM autocheck_reader;
GRANT CONNECT ON DATABASE course TO autocheck_reader;
-- Preserve the owner's extension access without exposing public to reader.
GRANT USAGE ON SCHEMA public TO course_owner;
REVOKE USAGE, CREATE ON SCHEMA public FROM PUBLIC;

-- Repeated administrative invocation strips only this reader's direct grants
-- and restores the same explicit view whitelist if migrations already exist.
DO $permissions$
DECLARE
    schema_name text;
    view_name text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY['public','catalog','api','payment','workflow','receipt','delivery','autocheck']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = schema_name) THEN
            EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA %I FROM autocheck_reader', schema_name);
            EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I FROM autocheck_reader', schema_name);
            EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM autocheck_reader', schema_name);
            EXECUTE format('REVOKE ALL ON SCHEMA %I FROM autocheck_reader', schema_name);
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'autocheck') THEN
        GRANT USAGE ON SCHEMA autocheck TO autocheck_reader;
        FOREACH view_name IN ARRAY ARRAY[
            'contract_info','action_definitions','action_dispatches','operations','operation_events',
            'flow_versions','processes','steps','jobs','attempts','signals','workflow_events',
            'external_requests','receipts','outbox','inbox','decisions']
        LOOP
            IF EXISTS (SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
                       WHERE n.nspname = 'autocheck' AND c.relname = view_name AND c.relkind = 'v') THEN
                EXECUTE format('GRANT SELECT ON autocheck.%I TO autocheck_reader', view_name);
            END IF;
        END LOOP;
    END IF;
END
$permissions$;
SQL
