from __future__ import annotations

from pathlib import Path
from typing import Any

from .intake import load_batch
from .normalize import Normalizer
from .quality import QualityGate
from .registry import ContractError, Registry
from .release import ReleaseBuilder
from .store import LedgerStore
from .compare import Comparator, SeriesWindow
from .audit import AuditInspector
from .coverage import CoverageReporter, BatchCoverage
from .replay import ReplayInspector
from .operations import ReviewQueue, ReceiptCatalog
from .baseline import BaselineManager


class LedgerService:
    def __init__(self, db_path: str | Path):
        self.store = LedgerStore(db_path)
        self.registry = Registry(self.store)
        self.normalizer = Normalizer(self.registry)
        self.quality = QualityGate(self.registry, self.store)
        self.releases = ReleaseBuilder(self.store)
        self.comparator = Comparator(self.store, self.registry)
        self.windows = SeriesWindow(self.store, self.registry)
        self.audit = AuditInspector(self.store, self.registry, self.releases)
        self.coverage_reporter = CoverageReporter(self.store, self.registry)
        self.batch_coverage = BatchCoverage(self.store, self.registry)
        self.replay_inspector = ReplayInspector(self.store, self.registry)
        self.queue = ReviewQueue(self.store)
        self.receipts = ReceiptCatalog(self.store, self.registry)
        self.baseline = BaselineManager(self.store, self.registry, self.quality)

    def close(self) -> None:
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def register_indicator(self, contract: dict[str, Any]) -> dict[str, Any]:
        indicator = self.registry.register_indicator(contract)
        return {"key": indicator.key, "canonical_unit": indicator.canonical_unit,
                "cadence": indicator.cadence}

    def register_source(self, contract: dict[str, Any]) -> dict[str, Any]:
        source = self.registry.register_source(contract)
        return {"key": source.key, "allowed_indicators": list(source.allowed_indicators)}

    def register_unit(self, contract: dict[str, Any]) -> dict[str, Any]:
        rule = self.registry.register_unit(contract)
        return {"indicator": rule.indicator, "source_unit": rule.source_unit,
                "factor": rule.factor, "offset": rule.offset}

    def ingest(self, manifest_path: str | Path) -> dict[str, Any]:
        envelope = load_batch(manifest_path)
        previous = self.store.latest_batch(envelope.source, envelope.batch_id)
        if previous and previous["payload_hash"] == envelope.payload_hash:
            # Same bytes: the batch and any baseline epoch already exist, so the
            # propagation call below only returns the existing round (no new epoch).
            batch = previous
            idempotent = True
        else:
            source = self.registry.source(envelope.source)
            if not source.active:
                raise ContractError("inactive_source", f"source {source.key} is inactive")
            measurements, rejected = self.normalizer.batch(source, envelope.rows)
            assessment = self.quality.assess(source.key, measurements, rejected)
            batch = self.store.insert_batch(envelope, assessment, measurements)
            idempotent = False
        propagation = self.baseline.propagate(batch)
        return {"batch": self.store.batch(batch["id"]), "idempotent": idempotent,
                "baseline": self._propagation_summary(propagation)}

    @staticmethod
    def _propagation_summary(propagation: dict[str, Any]) -> dict[str, Any]:
        reports = propagation["reports"]
        return {"reason": propagation["reason"], "epoch_count": len(reports),
                "epochs": [{"epoch_id": report["epoch"]["id"], "status": report["epoch"]["status"],
                            "indicator": report["epoch"]["indicator"],
                            "assessed_batches": len(report["assessments"]),
                            "confirmation_pending": sum(
                                1 for item in report["assessments"]
                                if item["needs_confirmation"] and item["confirmed_at"] is None)}
                           for report in reports]}

    def register_anomaly_rule(self, contract: dict[str, Any]) -> dict[str, Any]:
        return self.baseline.register_rule(contract)

    def baseline_epochs(self) -> list[dict[str, Any]]:
        return self.baseline.epochs()

    def baseline_epoch(self, epoch_id: int) -> dict[str, Any]:
        return self.baseline.epoch_report(epoch_id)

    def baseline_pending(self) -> list[dict[str, Any]]:
        return self.baseline.pending_confirmations()

    def confirm_baseline(self, assessment_id: int) -> dict[str, Any]:
        return self.baseline.confirm(assessment_id)

    def batch_baseline(self, batch_id: int) -> dict[str, Any]:
        return self.baseline.batch_baseline(batch_id)

    def approve(self, batch_id: int, expected_revision: int) -> dict[str, Any]:
        return self.store.approve(batch_id, expected_revision)

    def preview(self, batch_id: int, expected_revision: int) -> dict[str, Any]:
        return self.releases.preview(batch_id, expected_revision)

    def publish(self, batch_id: int, expected_revision: int) -> dict[str, Any]:
        # Publish guard: a refreshed-but-unconfirmed baseline verdict (or an open
        # correction epoch) is stale audit evidence and must not enter a release.
        self.baseline.assert_release_ready(batch_id)
        return self.releases.publish(batch_id, expected_revision)

    def export(self, version: int) -> dict[str, Any]:
        return self.releases.export(version)

    def verify(self, bundle: dict[str, Any]) -> bool:
        return self.releases.verify(bundle)

    def lineage(self, version: int) -> list[dict[str, Any]]:
        return self.releases.lineage(version)

    def batch(self, batch_id: int) -> dict[str, Any]:
        batch = self.store.batch(batch_id)
        batch["measurements"] = self.store.measurements(batch_id)
        return batch

    def batches(self, status: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_batches(status)

    def series(self, indicator: str, version: int | None = None,
               source: str | None = None) -> list[dict[str, Any]]:
        self.registry.indicator(indicator)
        if version is None:
            latest = self.store.latest_release()
            if latest is None:
                return []
            version = latest["version"]
        entries = self.store.release_entries(version)
        return [item for item in entries if item["indicator"] == indicator and
                (source is None or item["source"] == source)]

    def events(self, subject: str | None = None) -> list[dict[str, Any]]:
        return self.store.events(subject)

    def compare(self, old_version: int, new_version: int) -> dict[str, Any]:
        return self.comparator.releases(old_version, new_version)

    def impact(self, batch_id: int) -> dict[str, Any]:
        return self.comparator.batch_impact(batch_id)

    def statistics(self, indicator: str, version: int, source: str | None = None) -> dict[str, Any]:
        return self.windows.statistics(indicator, version, source=source)

    def audit_all(self) -> dict[str, Any]:
        return self.audit.inspect_all()

    def coverage(self, version: int, indicator: str | None = None,
                 source: str | None = None) -> dict[str, Any]:
        return self.coverage_reporter.report(version, indicator, source)

    def batch_coverage_report(self, batch_id: int) -> dict[str, Any]:
        return self.batch_coverage.report(batch_id)

    def discrepancies(self, version: int, indicator: str) -> dict[str, Any]:
        return self.coverage_reporter.discrepancy_report(version, indicator)

    def replay(self, manifest_path: str | Path, batch_id: int) -> dict[str, Any]:
        return self.replay_inspector.compare_manifest(manifest_path, batch_id)

    def pending_review(self, source: str | None = None) -> list[dict[str, Any]]:
        return self.queue.pending(source)

    def rejected_batches(self, source: str | None = None) -> list[dict[str, Any]]:
        return self.queue.rejected(source)

    def batch_history(self, source: str, external_id: str) -> list[dict[str, Any]]:
        return self.queue.state_history(source, external_id)

    def status(self) -> dict[str, Any]:
        latest = self.store.latest_release()
        batches = self.store.list_batches()
        return {"indicators": len(self.registry.list_indicators()),
                "sources": len(self.registry.list_sources()),
                "batches": len(batches),
                "by_status": {name: sum(row["status"] == name for row in batches)
                              for name in ("accepted", "review", "rejected", "approved", "published")},
                "latest_release": latest["version"] if latest else None}
