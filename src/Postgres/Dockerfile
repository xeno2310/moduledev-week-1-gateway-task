FROM postgres:17

COPY src/Postgres/000_week3_roles.sh /docker-entrypoint-initdb.d/000_week3_roles.sh

RUN chmod 0755 /docker-entrypoint-initdb.d/000_week3_roles.sh

COPY src/Postgres/000_workflow_worker.sql /docker-entrypoint-initdb.d/000_workflow_worker.sql
COPY src/Postgres/000_week4_roles.sh /docker-entrypoint-initdb.d/000_week4_roles.sh
COPY db/migrations/*.sql /docker-entrypoint-initdb.d/
COPY src/Postgres/course-entrypoint.sh /usr/local/bin/course-entrypoint.sh
RUN chmod 0755 /docker-entrypoint-initdb.d/000_week4_roles.sh /usr/local/bin/course-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/course-entrypoint.sh"]
CMD ["postgres"]
