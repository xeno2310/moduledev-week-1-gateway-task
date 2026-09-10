FROM postgres:17

COPY src/Postgres/000_week3_roles.sh /docker-entrypoint-initdb.d/000_week3_roles.sh

RUN chmod 0755 /docker-entrypoint-initdb.d/000_week3_roles.sh

COPY src/Postgres/000_workflow_worker.sql /docker-entrypoint-initdb.d/000_workflow_worker.sql
COPY db/migrations/ /docker-entrypoint-initdb.d/
