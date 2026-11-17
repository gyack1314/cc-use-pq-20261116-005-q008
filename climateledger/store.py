from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
import json
import sqlite3

from .registry import ContractError, UnitRule
from .intake import canonical_bytes


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS indicators (key TEXT PRIMARY KEY, contract TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (key TEXT PRIMARY KEY, contract TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unit_rules (
    indicator TEXT NOT NULL, source_unit TEXT NOT NULL, factor TEXT NOT NULL, offset TEXT NOT NULL,
    PRIMARY KEY(indicator, source_unit), FOREIGN KEY(indicator) REFERENCES indicators(key));
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL, revision INTEGER NOT NULL,
    payload_hash TEXT NOT NULL, status TEXT NOT NULL, row_count INTEGER NOT NULL,
    metadata TEXT NOT NULL, findings TEXT NOT NULL, received_at TEXT NOT NULL,
    approved_at TEXT, published_version INTEGER,
    UNIQUE(source, external_id, revision), FOREIGN KEY(source) REFERENCES sources(key));
CREATE INDEX IF NOT EXISTS batches_lookup ON batches(source, external_id, revision DESC);
CREATE TABLE IF NOT EXISTS measurements (
    batch_id INTEGER NOT NULL, line INTEGER NOT NULL, indicator TEXT NOT NULL, period TEXT NOT NULL,
    value TEXT NOT NULL, unit TEXT NOT NULL, raw_value TEXT NOT NULL, raw_unit TEXT NOT NULL,
    factor TEXT NOT NULL, offset TEXT NOT NULL,
    PRIMARY KEY(batch_id, line), FOREIGN KEY(batch_id) REFERENCES batches(id),
    FOREIGN KEY(indicator) REFERENCES indicators(key));
CREATE INDEX IF NOT EXISTS measurements_series ON measurements(indicator, period);
CREATE TABLE IF NOT EXISTS releases (
    version INTEGER PRIMARY KEY, created_at TEXT NOT NULL, manifest TEXT NOT NULL, digest TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS release_entries (
    version INTEGER NOT NULL, source TEXT NOT NULL, indicator TEXT NOT NULL, period TEXT NOT NULL,
    batch_id INTEGER NOT NULL, line INTEGER NOT NULL, value TEXT NOT NULL, unit TEXT NOT NULL,
    PRIMARY KEY(version, source, indicator, period), FOREIGN KEY(version) REFERENCES releases(version));
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, at TEXT NOT NULL, kind TEXT NOT NULL, subject TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS anomaly_rules (
    indicator TEXT PRIMARY KEY, sigma_threshold TEXT NOT NULL, min_baseline_points INTEGER NOT NULL,
    min_delta TEXT, created_at TEXT NOT NULL, FOREIGN KEY(indicator) REFERENCES indicators(key));
CREATE TABLE IF NOT EXISTS baseline_epochs (
    id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, trigger_batch_id INTEGER NOT NULL,
    trigger_revision INTEGER NOT NULL, source TEXT NOT NULL, indicator TEXT NOT NULL,
    corrected_periods TEXT NOT NULL, status TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
    FOREIGN KEY(trigger_batch_id) REFERENCES batches(id));
CREATE TABLE IF NOT EXISTS baseline_assessments (
    id INTEGER PRIMARY KEY, epoch_id INTEGER NOT NULL, batch_id INTEGER NOT NULL,
    prior_status TEXT NOT NULL, updated_status TEXT NOT NULL,
    prior_findings TEXT NOT NULL, updated_findings TEXT NOT NULL,
    jump_evidence TEXT NOT NULL, needs_confirmation INTEGER NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
    confirmed_at TEXT, created_at TEXT NOT NULL, UNIQUE(epoch_id, batch_id),
    FOREIGN KEY(epoch_id) REFERENCES baseline_epochs(id),
    FOREIGN KEY(batch_id) REFERENCES batches(id));
CREATE INDEX IF NOT EXISTS baseline_assessments_batch ON baseline_assessments(batch_id, epoch_id);
"""

# Each migration is (table, ALTER statement). The target table's existing
# columns decide whether the column still needs to be added.
SCHEMA_MIGRATIONS = (
    ("batches", "ALTER TABLE batches ADD COLUMN baseline_epoch_id INTEGER"),
    ("anomaly_rules", "ALTER TABLE anomaly_rules ADD COLUMN min_delta TEXT"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LedgerStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), timeout=20, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=20000")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        for table, statement in SCHEMA_MIGRATIONS:
            existing = {row["name"] for row in self.connection.execute(f"PRAGMA table_info({table})")}
            column = statement.split("ADD COLUMN", 1)[1].strip().split()[0]
            if column not in existing:
                self.connection.execute(statement)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def event(self, kind: str, subject: str, detail: dict[str, Any]) -> None:
        self.connection.execute("INSERT INTO events(at,kind,subject,detail) VALUES(?,?,?,?)",
                                (utc_now(), kind, subject, canonical_bytes(detail).decode()))

    def events(self, subject: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM events"
        args: tuple = ()
        if subject is not None:
            query += " WHERE subject=?"
            args = (subject,)
        query += " ORDER BY id"
        return [{**dict(row), "detail": json.loads(row["detail"])} for row in self.connection.execute(query, args)]

    def register_contract(self, table: str, key: str, contract: str) -> None:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        with self.transaction():
            existing = self.get_contract(table, key)
            if existing is not None and existing != contract:
                raise ContractError("contract_immutable", f"{table} {key} already has a different contract")
            if existing is None:
                self.connection.execute(f"INSERT INTO {table}(key,contract) VALUES(?,?)", (key, contract))
                self.event("register_" + table, key, json.loads(contract))

    def get_contract(self, table: str, key: str) -> str | None:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        row = self.connection.execute(f"SELECT contract FROM {table} WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def list_contracts(self, table: str) -> list[str]:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        return [row[0] for row in self.connection.execute(f"SELECT contract FROM {table} ORDER BY key")]

    def register_unit(self, rule: UnitRule) -> None:
        with self.transaction():
            old = self.get_unit(rule.indicator, rule.source_unit)
            if old is not None and old != rule:
                raise ContractError("rule_immutable", "unit rule already differs")
            if old is None:
                self.connection.execute("INSERT INTO unit_rules VALUES(?,?,?,?)",
                                        (rule.indicator, rule.source_unit, rule.factor, rule.offset))
                self.event("register_unit", rule.indicator, asdict(rule))

    def get_unit(self, indicator: str, source_unit: str) -> UnitRule | None:
        row = self.connection.execute("SELECT * FROM unit_rules WHERE indicator=? AND source_unit=?",
                                      (indicator, source_unit)).fetchone()
        return UnitRule(*row) if row else None

    def latest_batch(self, source: str, external_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM batches WHERE source=? AND external_id=? ORDER BY revision DESC LIMIT 1",
                                      (source, external_id)).fetchone()
        return self._batch(row) if row else None

    def batch(self, batch_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise ContractError("unknown_batch", f"batch {batch_id} does not exist")
        return self._batch(row)

    def _batch(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["metadata"] = json.loads(result["metadata"])
        result["findings"] = json.loads(result["findings"])
        return result

    def list_batches(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute("SELECT * FROM batches WHERE status=? ORDER BY id", (status,))
        else:
            rows = self.connection.execute("SELECT * FROM batches ORDER BY id")
        return [self._batch(row) for row in rows]

    def insert_batch(self, envelope, assessment, measurements) -> dict[str, Any]:
        with self.transaction():
            old = self.latest_batch(envelope.source, envelope.batch_id)
            if old and old["payload_hash"] == envelope.payload_hash:
                return old
            revision = 1 if old is None else old["revision"] + 1
            cursor = self.connection.execute(
                "INSERT INTO batches(source,external_id,revision,payload_hash,status,row_count,metadata,findings,received_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (envelope.source, envelope.batch_id, revision, envelope.payload_hash,
                 assessment.status, envelope.declared_count,
                 canonical_bytes(envelope.metadata).decode(),
                 canonical_bytes([item.document() for item in assessment.findings]).decode(), utc_now()))
            batch_id = cursor.lastrowid
            self.connection.executemany(
                "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?,?,?)",
                [(batch_id, item.line, item.indicator, item.period, item.value, item.unit,
                  item.raw_value, item.raw_unit, item.rule_factor, item.rule_offset)
                 for item in measurements])
            self.event("ingest", f"batch:{batch_id}", {"status": assessment.status,
                       "revision": revision, "payload_hash": envelope.payload_hash})
            return self.batch(batch_id)

    def approve(self, batch_id: int, expected_revision: int) -> dict[str, Any]:
        with self.transaction():
            batch = self.batch(batch_id)
            latest = self.latest_batch(batch["source"], batch["external_id"])
            if latest["id"] != batch_id or batch["revision"] != expected_revision:
                raise ContractError("stale_revision", "batch is not the latest expected revision")
            if batch["status"] == "approved":
                return batch
            if batch["status"] != "review":
                raise ContractError("not_reviewable", "only review batches can be approved")
            self.connection.execute("UPDATE batches SET status='approved',approved_at=? WHERE id=?",
                                    (utc_now(), batch_id))
            self.event("approve", f"batch:{batch_id}", {"revision": expected_revision})
            return self.batch(batch_id)

    def measurements(self, batch_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM measurements WHERE batch_id=? ORDER BY line", (batch_id,))]

    def current_measurement(self, source: str, indicator: str, period: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT e.* FROM release_entries e JOIN releases r ON r.version=e.version "
            "WHERE e.source=? AND e.indicator=? AND e.period=? ORDER BY e.version DESC LIMIT 1",
            (source, indicator, period)).fetchone()
        return dict(row) if row else None

    def latest_release(self) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM releases ORDER BY version DESC LIMIT 1").fetchone()
        return self._release(row) if row else None

    def release(self, version: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM releases WHERE version=?", (version,)).fetchone()
        if row is None:
            raise ContractError("unknown_release", f"release {version} does not exist")
        return self._release(row)

    def _release(self, row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["manifest"] = json.loads(value["manifest"])
        return value

    def release_entries(self, version: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM release_entries WHERE version=? ORDER BY source,indicator,period", (version,))]

    def publish(self, batch_id: int, expected_revision: int, manifest: dict[str, Any],
                entries: list[dict[str, Any]], digest: str) -> dict[str, Any]:
        with self.transaction():
            batch = self.batch(batch_id)
            latest = self.latest_batch(batch["source"], batch["external_id"])
            if batch["published_version"]:
                return self.release(batch["published_version"])
            if latest["id"] != batch_id or batch["revision"] != expected_revision:
                raise ContractError("stale_revision", "cannot publish a superseded revision")
            if batch["status"] not in {"accepted", "approved"}:
                raise ContractError("quality_gate", "batch is not accepted or approved")
            current_version = self.latest_release()["version"] if self.latest_release() else 0
            version = current_version + 1
            if manifest["version"] != version:
                raise ContractError("release_race", "release version changed while preparing")
            self.connection.execute("INSERT INTO releases VALUES(?,?,?,?)",
                                    (version, utc_now(), canonical_bytes(manifest).decode(), digest))
            self.connection.executemany("INSERT INTO release_entries VALUES(?,?,?,?,?,?,?,?)",
                [(version, item["source"], item["indicator"], item["period"],
                  item["batch_id"], item["line"], item["value"], item["unit"]) for item in entries])
            self.connection.execute("UPDATE batches SET status='published',published_version=? WHERE id=?",
                                    (version, batch_id))
            self.event("publish", f"batch:{batch_id}", {"version": version, "digest": digest})
            return self.release(version)

    # ------------------------------------------------------------------
    # Reference baseline: anomaly rules, epochs and re-assessments
    # ------------------------------------------------------------------
    def upsert_anomaly_rule(self, indicator: str, sigma_threshold: str,
                            min_baseline_points: int, min_delta: str | None = None) -> dict[str, Any]:
        with self.transaction():
            self.connection.execute(
                "INSERT INTO anomaly_rules(indicator,sigma_threshold,min_baseline_points,min_delta,created_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(indicator) DO UPDATE SET "
                "sigma_threshold=excluded.sigma_threshold, "
                "min_baseline_points=excluded.min_baseline_points, "
                "min_delta=excluded.min_delta",
                (indicator, sigma_threshold, min_baseline_points, min_delta, utc_now()))
            self.event("register_anomaly_rule", indicator,
                       {"sigma_threshold": sigma_threshold, "min_baseline_points": min_baseline_points,
                        "min_delta": min_delta})
            return self.get_anomaly_rule(indicator)

    def get_anomaly_rule(self, indicator: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM anomaly_rules WHERE indicator=?", (indicator,)).fetchone()
        return dict(row) if row else None

    def list_anomaly_rules(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM anomaly_rules ORDER BY indicator")]

    def baseline_epoch(self, epoch_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM baseline_epochs WHERE id=?", (epoch_id,)).fetchone()
        if row is None:
            raise ContractError("unknown_epoch", f"baseline epoch {epoch_id} does not exist")
        result = dict(row)
        result["corrected_periods"] = json.loads(result["corrected_periods"])
        return result

    def list_baseline_epochs(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM baseline_epochs ORDER BY id")
        results = []
        for row in rows:
            result = dict(row)
            result["corrected_periods"] = json.loads(result["corrected_periods"])
            results.append(result)
        return results

    def insert_baseline_epoch(self, *, trigger_batch_id: int, trigger_revision: int, source: str,
                              indicator: str, corrected_periods: list[dict[str, Any]],
                              idempotency_key: str, status: str = "open") -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO baseline_epochs(created_at,trigger_batch_id,trigger_revision,source,indicator,"
            "corrected_periods,status,idempotency_key) VALUES(?,?,?,?,?,?,?,?)",
            (utc_now(), trigger_batch_id, trigger_revision, source, indicator,
             canonical_bytes(corrected_periods).decode(), status, idempotency_key))
        epoch_id = cursor.lastrowid
        self.event("baseline_epoch", f"epoch:{epoch_id}",
                   {"trigger_batch_id": trigger_batch_id, "source": source, "indicator": indicator,
                    "corrected_periods": corrected_periods, "idempotency_key": idempotency_key})
        return self.baseline_epoch(epoch_id)

    def mark_baseline_epoch(self, epoch_id: int, status: str) -> None:
        self.connection.execute("UPDATE baseline_epochs SET status=? WHERE id=?", (status, epoch_id))
        self.event("baseline_epoch_status", f"epoch:{epoch_id}", {"status": status})

    def record_baseline_assessment(self, *, epoch_id: int, batch_id: int, prior_status: str,
                                   updated_status: str, prior_findings: list[dict[str, Any]],
                                   updated_findings: list[dict[str, Any]],
                                   jump_evidence: list[dict[str, Any]], needs_confirmation: bool,
                                   applied: bool) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO baseline_assessments(epoch_id,batch_id,prior_status,updated_status,"
            "prior_findings,updated_findings,jump_evidence,needs_confirmation,applied,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (epoch_id, batch_id, prior_status, updated_status,
             canonical_bytes(prior_findings).decode(), canonical_bytes(updated_findings).decode(),
             canonical_bytes(jump_evidence).decode(), 1 if needs_confirmation else 0,
             1 if applied else 0, utc_now()))
        return self.baseline_assessment(cursor.lastrowid)

    def baseline_assessment(self, assessment_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM baseline_assessments WHERE id=?",
                                      (assessment_id,)).fetchone()
        if row is None:
            raise ContractError("unknown_assessment", f"baseline assessment {assessment_id} does not exist")
        result = self._assessment_row(row)
        result["confirmed_at"] = row["confirmed_at"]
        return result

    def baseline_assessments_for_epoch(self, epoch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM baseline_assessments WHERE epoch_id=? ORDER BY batch_id", (epoch_id,))
        return [self._assessment_row(row) for row in rows]

    def latest_baseline_assessment(self, batch_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM baseline_assessments WHERE batch_id=? ORDER BY epoch_id DESC, id DESC LIMIT 1",
            (batch_id,)).fetchone()
        if row is None:
            return None
        return self._assessment_row(row)

    def unconfirmed_assessments(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM baseline_assessments WHERE batch_id=? AND needs_confirmation=1 "
            "AND confirmed_at IS NULL ORDER BY epoch_id, id", (batch_id,))
        return [self._assessment_row(row) for row in rows]

    def _assessment_row(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in ("prior_findings", "updated_findings", "jump_evidence"):
            result[field] = json.loads(result[field])
        result["needs_confirmation"] = bool(result["needs_confirmation"])
        result["applied"] = bool(result["applied"])
        return result

    def mark_batch_baseline(self, batch_id: int, epoch_id: int, status: str,
                            findings: list[dict[str, Any]]) -> None:
        self.connection.execute(
            "UPDATE batches SET baseline_epoch_id=?, status=?, findings=? WHERE id=?",
            (epoch_id, status, canonical_bytes(findings).decode(), batch_id))

    def confirm_baseline_assessment(self, assessment_id: int) -> None:
        self.connection.execute(
            "UPDATE baseline_assessments SET confirmed_at=? WHERE id=? AND confirmed_at IS NULL",
            (utc_now(), assessment_id))

    def epochs_for_trigger(self, trigger_batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM baseline_epochs WHERE trigger_batch_id=? ORDER BY id", (trigger_batch_id,))
        results = []
        for row in rows:
            result = dict(row)
            result["corrected_periods"] = json.loads(result["corrected_periods"])
            results.append(result)
        return results

    def batches_for_series(self, source: str, indicator: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT DISTINCT b.* FROM batches b JOIN measurements m ON m.batch_id=b.id "
            "WHERE b.source=? AND m.indicator=? ORDER BY b.id", (source, indicator))
        return [self._batch(row) for row in rows]
