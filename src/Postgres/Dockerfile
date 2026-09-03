FROM postgres:17

COPY src/Postgres/000_workflow_worker.sql /docker-entrypoint-initdb.d/000_workflow_worker.sql
COPY db/migrations/ /docker-entrypoint-initdb.d/