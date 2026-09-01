SET ROLE course_owner;

-- api.invoke имеет намеренно ограниченный search_path:
-- pg_catalog, api, catalog.
--
-- pgcrypto установлен в public, поэтому создаём доверенный
-- bridge внутри api вместо добавления public в search_path.

CREATE OR REPLACE FUNCTION api.digest(
    p_data bytea,
    p_type text
)
RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT public.digest(p_data, p_type);
$$;

REVOKE ALL
ON FUNCTION api.digest(bytea, text)
FROM PUBLIC;

RESET ROLE;