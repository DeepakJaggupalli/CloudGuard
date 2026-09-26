import sqlite3
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "remediation" / "feedback.db"


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                vm_id TEXT,
                anomaly_type TEXT,
                recommended_remedy TEXT,
                confidence REAL,
                priority TEXT,
                approval TEXT,
                execution_status TEXT,
                operator_note TEXT,
                timestamp TEXT
            )
        """)
        conn.commit()


def store_feedback(
    event_id,
    vm_id,
    anomaly_type,
    recommended_remedy,
    confidence,
    priority,
    approval,
    execution_status,
    operator_note=""
):
    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO feedback (
                event_id,
                vm_id,
                anomaly_type,
                recommended_remedy,
                confidence,
                priority,
                approval,
                execution_status,
                operator_note,
                timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            vm_id,
            anomaly_type,
            recommended_remedy,
            confidence,
            priority,
            approval,
            execution_status,
            operator_note,
            datetime.utcnow().isoformat() + "Z"
        ))
        conn.commit()


def get_feedback(limit=100):
    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT *
            FROM feedback
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

        return [dict(row) for row in rows]


def get_training_feedback():
    """
    Return operator decisions that can be used as supervised
    feedback for future remedy-model retraining.

    Approved  -> recommended remedy accepted.
    Rejected  -> recommended remedy rejected, therefore excluded
                 from positive remedy labels.
    """

    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT
                anomaly_type,
                recommended_remedy,
                priority,
                approval,
                confidence
            FROM feedback
            WHERE approval = 'approved'
              AND recommended_remedy IS NOT NULL
              AND recommended_remedy != ''
        """).fetchall()

        return [dict(row) for row in rows]
