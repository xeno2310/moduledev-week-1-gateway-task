from __future__ import annotations

import os
import time

import psycopg


def _interval() -> float:
    return (
        0.10
        if os.getenv(
            "COURSE_TEST_PROFILE",
            "0",
        ) == "1"
        else 0.40
    )


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
