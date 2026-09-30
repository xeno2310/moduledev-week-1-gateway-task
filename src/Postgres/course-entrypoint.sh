#!/bin/sh
set -eu

# Delegate utility commands unchanged, including the official option shorthand.
case "${1:-}" in
    -*) set -- postgres "$@" ;;
esac
if [ "${1:-}" != postgres ]; then
    exec /usr/local/bin/docker-entrypoint.sh "$@"
fi

read_integer() {
    setting_name=$1
    setting_value=$2
    setting_min=$3
    setting_max=$4
    case "$setting_value" in
        ''|*[!0-9]*) printf '%s: invalid integer configuration\n' "$setting_name" >&2; exit 1 ;;
    esac
    if [ "${#setting_value}" -gt 9 ]; then
        printf '%s: integer configuration out of range\n' "$setting_name" >&2
        exit 1
    fi
    # Normalize decimal leading zeros before any shell arithmetic/comparison.
    setting_value=$(printf '%s' "$setting_value" | sed 's/^0*//; s/^$/0/')
    if [ "$setting_value" -lt "$setting_min" ] || [ "$setting_value" -gt "$setting_max" ]; then
        printf '%s: integer configuration out of range\n' "$setting_name" >&2
        exit 1
    fi
    printf '%s' "$setting_value"
}

if [ "${COURSE_TEST_PROFILE:-0}" = 1 ]; then
    base_default=200
    max_default=800
    jitter_default=100
    lease_default=2000
    job_default=2000
else
    base_default=1000
    max_default=30000
    jitter_default=1000
    lease_default=20000
    job_default=10000
fi

# '-' (not ':-') preserves explicit empty values so validation fails closed.
attempts=$(read_integer COURSE_OUTBOX_MAX_ATTEMPTS "${COURSE_OUTBOX_MAX_ATTEMPTS-4}" 1 100)
base=$(read_integer COURSE_OUTBOX_BACKOFF_BASE_MS "${COURSE_OUTBOX_BACKOFF_BASE_MS-$base_default}" 1 300000)
maximum=$(read_integer COURSE_OUTBOX_BACKOFF_MAX_MS "${COURSE_OUTBOX_BACKOFF_MAX_MS-$max_default}" 1 300000)
jitter=$(read_integer COURSE_OUTBOX_JITTER_MAX_MS "${COURSE_OUTBOX_JITTER_MAX_MS-$jitter_default}" 0 300000)
lease=$(read_integer COURSE_OUTBOX_LEASE_MS "${COURSE_OUTBOX_LEASE_MS-$lease_default}" 100 300000)
job=$(read_integer COURSE_JOB_LEASE_MS "${COURSE_JOB_LEASE_MS-$job_default}" 100 300000)
if [ "$base" -gt "$maximum" ]; then
    printf '%s\n' 'COURSE_OUTBOX_BACKOFF_BASE_MS must not exceed COURSE_OUTBOX_BACKOFF_MAX_MS' >&2
    exit 1
fi

# Custom GUCs are trusted deployment configuration, not an authorization
# boundary: PostgreSQL placeholders remain session-settable. Runtime code
# never SETs these policy parameters. Only these six allowlisted names pass.
exec /usr/local/bin/docker-entrypoint.sh "$@" \
    -c "course.outbox_max_attempts=$attempts" \
    -c "course.outbox_backoff_base_ms=$base" \
    -c "course.outbox_backoff_max_ms=$maximum" \
    -c "course.outbox_jitter_max_ms=$jitter" \
    -c "course.outbox_lease_ms=$lease" \
    -c "course.job_lease_ms=$job"
