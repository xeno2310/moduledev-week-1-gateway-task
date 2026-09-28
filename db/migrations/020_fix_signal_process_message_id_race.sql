SET ROLE course_owner;

CREATE OR REPLACE FUNCTION workflow.signal_process(
    p_process_id uuid,
    p_signal_type text,
    p_message_id text,
    p_body jsonb,
    p_body_hash text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_process workflow.process_instances%ROWTYPE;
    v_existing workflow.workflow_signals%ROWTYPE;

    v_step_instance_id uuid;
    v_outcome text;
    v_next_step text;
BEGIN
    SELECT *
    INTO v_existing
    FROM workflow.workflow_signals
    WHERE message_id = p_message_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.process_id = p_process_id
           AND v_existing.signal_type = p_signal_type
           AND v_existing.body_hash = p_body_hash
           AND v_existing.body = COALESCE(p_body, '{}'::jsonb)
        THEN
            RETURN jsonb_build_object(
                'status', 'ok',
                'result', jsonb_build_object(
                    'resource', 'signal',
                    'processId', p_process_id,
                    'messageId', p_message_id,
                    'signalType', p_signal_type,
                    'status', 'duplicate'
                ),
                'meta', jsonb_build_object(
                    'contractVersion', 'course-1'
                )
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.signal_conflict',
            'message', 'message id already belongs to another signal',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = p_process_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.process_not_found',
            'message', 'process does not exist',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    -- Only signal types declared by the pinned map are accepted.
    IF NOT EXISTS (
        SELECT 1
        FROM workflow.step_definitions AS s
        WHERE s.flow_name = v_process.flow_name
          AND s.flow_version = v_process.flow_version
          AND s.step_type = 'WAIT_SIGNAL'
          AND s.signal_type = p_signal_type
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.signal_unknown',
            'message', 'signal type is not declared by pinned flow',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    INSERT INTO workflow.workflow_signals
    (
        message_id,
        process_id,
        signal_type,
        body,
        body_hash,
        status
    )
    VALUES
    (
        p_message_id,
        p_process_id,
        p_signal_type,
        COALESCE(p_body, '{}'::jsonb),
        p_body_hash,
        'ACCEPTED'
    )
    ON CONFLICT (message_id) DO NOTHING;

    IF NOT FOUND THEN
        SELECT *
        INTO v_existing
        FROM workflow.workflow_signals
        WHERE message_id = p_message_id;

        IF v_existing.process_id = p_process_id
           AND v_existing.signal_type = p_signal_type
           AND v_existing.body_hash = p_body_hash
           AND v_existing.body = COALESCE(p_body, '{}'::jsonb)
        THEN
            RETURN jsonb_build_object(
                'status', 'ok',
                'result', jsonb_build_object(
                    'resource', 'signal',
                    'processId', p_process_id,
                    'messageId', p_message_id,
                    'signalType', p_signal_type,
                    'status', 'duplicate'
                ),
                'meta', jsonb_build_object(
                    'contractVersion', 'course-1'
                )
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.signal_conflict',
            'message', 'message id already belongs to another signal',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    INSERT INTO workflow.workflow_events
    (
        event_id,
        process_id,
        step_instance_id,
        event_type,
        metadata
    )
    VALUES
    (
        gen_random_uuid(),
        p_process_id,
        NULL,
        'SignalAccepted',
        jsonb_build_object(
            'messageId', p_message_id,
            'signalType', p_signal_type
        )
    );

    -- If the process is already at the matching wait step,
    -- consume the signal immediately.
    IF v_process.state = 'WAITING_SIGNAL' THEN
        SELECT
            d.declared_outcome,
            i.step_instance_id
        INTO
            v_outcome,
            v_step_instance_id
        FROM workflow.step_definitions AS d
        JOIN workflow.step_instances AS i
          ON i.process_id = p_process_id
         AND i.step_key = d.step_key
         AND i.state = 'WAITING'
        WHERE d.flow_name = v_process.flow_name
          AND d.flow_version = v_process.flow_version
          AND d.step_key = v_process.current_step_key
          AND d.step_type = 'WAIT_SIGNAL'
          AND d.signal_type = p_signal_type
        ORDER BY i.entered_at DESC
        LIMIT 1
        FOR UPDATE OF i;

        IF FOUND THEN
            UPDATE workflow.workflow_signals
            SET
                status = 'APPLIED',
                applied_at = clock_timestamp()
            WHERE message_id = p_message_id;

            UPDATE workflow.step_instances
            SET
                state = 'COMPLETED',
                outcome = v_outcome,
                completed_at = clock_timestamp()
            WHERE step_instance_id = v_step_instance_id;

            INSERT INTO workflow.workflow_events
            (
                event_id,
                process_id,
                step_instance_id,
                event_type,
                metadata
            )
            VALUES
            (
                gen_random_uuid(),
                p_process_id,
                v_step_instance_id,
                'SignalApplied',
                jsonb_build_object(
                    'messageId', p_message_id,
                    'signalType', p_signal_type
                )
            );

            SELECT to_step
            INTO v_next_step
            FROM workflow.transition_definitions
            WHERE flow_name = v_process.flow_name
              AND flow_version = v_process.flow_version
              AND from_step = v_process.current_step_key
              AND outcome = v_outcome;

            IF v_next_step IS NULL THEN
                RAISE EXCEPTION 'workflow transition does not exist';
            END IF;

            PERFORM workflow._enter_step(
                p_process_id,
                v_next_step
            );
        END IF;
    END IF;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'signal',
            'processId', p_process_id,
            'messageId', p_message_id,
            'signalType', p_signal_type,
            'status', 'accepted'
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;

RESET ROLE;
