SET ROLE course_owner;

-- ============================================================
-- Publish immutable workflow version
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.publish_flow(
    p_document jsonb,
    p_source_hash text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_flow_name text;
    v_version integer;
    v_start_step text;

    v_existing_hash text;

    v_step jsonb;
    v_transition jsonb;

    v_type text;
BEGIN
    v_flow_name := p_document ->> 'flow_name';
    v_version := (p_document ->> 'version')::integer;
    v_start_step := p_document ->> 'start_step';

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            v_flow_name || chr(31) || v_version::text,
            0
        )
    );

    SELECT source_hash
    INTO v_existing_hash
    FROM workflow.flow_versions
    WHERE flow_name = v_flow_name
      AND flow_version = v_version
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing_hash = p_source_hash THEN
            RETURN jsonb_build_object(
                'status', 'ok',
                'result', jsonb_build_object(
                    'resource', 'flow',
                    'operation', 'published',
                    'flowName', v_flow_name,
                    'flowVersion', v_version
                ),
                'meta', jsonb_build_object(
                    'contractVersion', 'course-1'
                )
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.publish_conflict',
            'message',
                'flow version already exists with different contract',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    INSERT INTO workflow.flow_definitions
    (
        flow_name
    )
    VALUES
    (
        v_flow_name
    )
    ON CONFLICT (flow_name)
    DO NOTHING;

    INSERT INTO workflow.flow_versions
    (
        flow_name,
        flow_version,
        status,
        is_active,
        start_step,
        source_hash,
        source_document
    )
    VALUES
    (
        v_flow_name,
        v_version,
        'PUBLISHED',
        false,
        v_start_step,
        p_source_hash,
        p_document
    );

    FOR v_step IN
        SELECT value
        FROM jsonb_array_elements(
            p_document -> 'steps'
        )
    LOOP
        v_type :=
            upper(v_step ->> 'type');

        INSERT INTO workflow.step_definitions
        (
            flow_name,
            flow_version,
            step_key,
            step_type,
            signal_type,
            declared_outcome,
            allowed_outcomes
        )
        VALUES
        (
            v_flow_name,
            v_version,
            v_step ->> 'key',
            v_type,

            CASE
                WHEN v_type = 'WAIT_SIGNAL'
                THEN v_step ->> 'signal_type'
                ELSE NULL
            END,

            CASE
                WHEN v_type IN ('WAIT_SIGNAL', 'END')
                THEN v_step ->> 'outcome'
                ELSE NULL
            END,

            CASE
                WHEN v_type = 'MANUAL'
                THEN v_step -> 'allowed_outcomes'
                ELSE NULL
            END
        );

        IF v_type = 'AUTOMATIC' THEN
            INSERT INTO workflow.task_definitions
            (
                flow_name,
                flow_version,
                step_key,

                service,
                module,
                action,
                action_version,

                required_policy,

                timeout_ms,
                max_attempts,
                delays_ms,

                input_mapping,
                input_constants
            )
            VALUES
            (
                v_flow_name,
                v_version,
                v_step ->> 'key',

                v_step #>> '{task,service}',
                v_step #>> '{task,module}',
                v_step #>> '{task,action}',
                (v_step #>> '{task,action_version}')::integer,

                v_step #> '{task,required_policy}',

                (v_step #>> '{task,timeout_ms}')::integer,
                (v_step #>> '{task,retry,max_attempts}')::integer,
                v_step #> '{task,retry,delays_ms}',

                v_step #> '{task,input_mapping}',
                v_step #> '{task,input_constants}'
            );
        END IF;
    END LOOP;

    FOR v_transition IN
        SELECT value
        FROM jsonb_array_elements(
            p_document -> 'transitions'
        )
    LOOP
        INSERT INTO workflow.transition_definitions
        (
            flow_name,
            flow_version,
            from_step,
            outcome,
            to_step
        )
        VALUES
        (
            v_flow_name,
            v_version,
            v_transition ->> 'from',
            v_transition ->> 'outcome',
            v_transition ->> 'to'
        );
    END LOOP;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'flow',
            'operation', 'published',
            'flowName', v_flow_name,
            'flowVersion', v_version
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Atomic activation
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.activate_flow(
    p_flow_name text,
    p_version integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            p_flow_name,
            0
        )
    );

    IF NOT EXISTS (
        SELECT 1
        FROM workflow.flow_versions
        WHERE flow_name = p_flow_name
          AND flow_version = p_version
          AND status = 'PUBLISHED'
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.flow_not_found',
            'message', 'published flow version does not exist',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    UPDATE workflow.flow_versions
    SET is_active = false
    WHERE flow_name = p_flow_name
      AND is_active = true;

    UPDATE workflow.flow_versions
    SET is_active = true
    WHERE flow_name = p_flow_name
      AND flow_version = p_version;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'flow',
            'operation', 'activated',
            'flowName', p_flow_name,
            'flowVersion', p_version
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- List published versions
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.list_flows()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
    SELECT jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'items',
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'flowName', v.flow_name,
                        'flowVersion', v.flow_version,
                        'status', v.status,
                        'isActive', v.is_active,
                        'publishedAt', v.published_at
                    )
                    ORDER BY
                        v.flow_name,
                        v.flow_version
                ),
                '[]'::jsonb
            )
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    )
    FROM workflow.flow_versions AS v;
$$;


-- ============================================================
-- Publication / CLI privileges
-- ============================================================

REVOKE ALL
ON FUNCTION workflow.publish_flow(jsonb, text)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION workflow.activate_flow(text, integer)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION workflow.list_flows()
FROM PUBLIC;

GRANT EXECUTE
ON FUNCTION workflow.publish_flow(jsonb, text)
TO course_publication;

GRANT EXECUTE
ON FUNCTION workflow.activate_flow(text, integer)
TO course_publication;

GRANT EXECUTE
ON FUNCTION workflow.list_flows()
TO course_publication;

-- Semantic validation needs the published action contract.
GRANT SELECT
ON catalog.action_definitions
TO course_publication;

-- Only CLI test-finish uses this and the command itself is
-- additionally gated by COURSE_TEST_PROFILE=1.
GRANT EXECUTE
ON FUNCTION workflow.finish_job(
    uuid,
    text,
    bigint,
    text,
    jsonb
)
TO course_publication;

RESET ROLE;