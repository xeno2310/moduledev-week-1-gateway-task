SET ROLE course_owner;

GRANT USAGE
ON SCHEMA catalog
TO course_publication;

GRANT SELECT, INSERT
ON TABLE catalog.action_definitions
TO course_publication;

GRANT UPDATE (enabled, is_default)
ON TABLE catalog.action_definitions
TO course_publication;

RESET ROLE;