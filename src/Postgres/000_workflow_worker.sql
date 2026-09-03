DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'workflow_worker'
    ) THEN
        CREATE ROLE workflow_worker
            LOGIN
            PASSWORD 'workflow-worker-local-only';
    END IF;
END
$$;