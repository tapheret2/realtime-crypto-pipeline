"""Bootstrap connections + variables Airflow needs.

Sourced once by the ``airflow-init`` container after ``airflow db migrate``.
Keeping this in code means the project is reproducible — drop the volume,
``docker compose up`` and the DAGs are wired up again automatically.
"""

from __future__ import annotations

import os

from airflow.models import Connection
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session


@provide_session
def upsert_connection(conn: Connection, session: Session = None) -> None:  # type: ignore[assignment]
    existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if existing is None:
        session.add(conn)
    else:
        existing.conn_type = conn.conn_type
        existing.host = conn.host
        existing.schema = conn.schema
        existing.login = conn.login
        existing.set_password(conn.password)
        existing.port = conn.port
    session.commit()


def main() -> None:
    upsert_connection(
        Connection(
            conn_id="crypto_pg",
            conn_type="postgres",
            host=os.getenv("POSTGRES_HOST", "postgres"),
            schema=os.getenv("POSTGRES_DB", "crypto"),
            login=os.getenv("POSTGRES_USER", "crypto"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
        )
    )


if __name__ == "__main__":
    main()
