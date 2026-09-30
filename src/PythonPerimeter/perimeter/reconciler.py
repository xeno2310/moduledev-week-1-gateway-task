from __future__ import annotations

import time

import psycopg

from .common import inbox_poll_ms, observability_server, postgres_ready

def _interval() -> float:
    return inbox_poll_ms() / 1000.0


def _run_connection() -> None:
    interval = _interval()

    with psycopg.connect(
        "",
        autocommit=True,
    ) as connection:

        while True:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT delivery.reconcile_inbox(
                        %s
                    );
                    """,
                    (50,),
                )

                applied = cur.fetchone()[0]

            if not applied:
                time.sleep(
                    interval
                )


def main() -> None:
    interval = _interval()

    with observability_server(postgres_ready):
        while True:
            try:
                _run_connection()
            except (
                psycopg.Error,
                OSError,
            ):
                time.sleep(
                    interval
                )


if __name__ == "__main__":
    main()
