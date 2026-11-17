from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
from typing import Any

from .intake import canonical_bytes
from .normalize import Measurement, period_index
from .quality import JumpDetector
from .registry import ContractError, Registry
from .store import LedgerStore


# Finding codes whose verdict is derived from a reference baseline. These are
# regenerated on every propagation round; every other finding (blocking checks,
# missing periods, revised values) belongs to the original audit rationale and
# is preserved verbatim.
BASELINE_FINDING_CODES = {"baseline_jump"}


class BaselineManager:
    """Propagate an upstream correction through the reference baseline.

    A correction batch revises already-published observations. Each distinct
    correction opens one immutable *baseline epoch* keyed by the batch identity
    and payload hash, so repeated deliveries and interrupted retries never open
    a second round. The epoch records, for every affected downstream batch, the
    previous and regenerated quality verdicts alongside the numeric jump
    evidence: old snapshots and their audit rationale stay queryable while the
    current verdict is refreshed against the corrected baseline.
    """

    def __init__(self, store: LedgerStore, registry: Registry, quality):
        self.store = store
        self.registry = registry
        self.quality = quality

    # ------------------------------------------------------------------
    # Anomaly rules
    # ------------------------------------------------------------------
    def register_rule(self, contract: dict[str, Any]) -> dict[str, Any]:
        indicator = self.registry.indicator(contract["indicator"])
        threshold = contract.get("sigma_threshold", 3.0)
        try:
            sigma = Decimal(str(threshold))
        except Exception:
            raise ContractError("anomaly_rule", "sigma_threshold must be a decimal") from None
        if not sigma.is_finite() or sigma <= 0:
            raise ContractError("anomaly_rule", "sigma_threshold must be a positive finite decimal")
        minimum = contract.get("min_baseline_points", 3)
        if type(minimum) is not int or not 1 <= minimum <= 1000:
            raise ContractError("anomaly_rule", "min_baseline_points must be an integer from 1 to 1000")
        min_delta = None
        if contract.get("min_delta") is not None:
            try:
                min_delta = Decimal(str(contract["min_delta"]))
            except Exception:
                raise ContractError("anomaly_rule", "min_delta must be a decimal") from None
            if not min_delta.is_finite() or min_delta < 0:
                raise ContractError("anomaly_rule", "min_delta must be a non-negative finite decimal")
            min_delta = str(min_delta)
        rule = self.store.upsert_anomaly_rule(indicator.key, str(sigma), minimum, min_delta)
        return rule

    def rules(self) -> list[dict[str, Any]]:
        return self.store.list_anomaly_rules()

    # ------------------------------------------------------------------
    # Correction propagation
    # ------------------------------------------------------------------
    @staticmethod
    def idempotency_key(batch: dict[str, Any]) -> str:
        raw = canonical_bytes({"source": batch["source"], "external_id": batch["external_id"],
                               "revision": batch["revision"], "payload_hash": batch["payload_hash"]})
        return sha256(raw).hexdigest()

    def corrected_periods(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        corrections: list[dict[str, Any]] = []
        for item in self.store.measurements(batch["id"]):
            prior = self.store.current_measurement(batch["source"], item["indicator"], item["period"])
            if prior is not None and prior["value"] != item["value"]:
                corrections.append({"indicator": item["indicator"], "period": item["period"],
                                    "old_value": prior["value"], "new_value": item["value"],
                                    "old_batch": prior["batch_id"], "new_batch": batch["id"]})
        return corrections

    def propagate(self, batch: dict[str, Any]) -> dict[str, Any]:
        """Open (or reuse) the epoch triggered by ``batch`` and re-judge downstream.

        Returns the epoch document with its assessments. Safe to call again with
        the same batch: epochs already opened by that batch are returned as-is.
        """
        existing = self.store.epochs_for_trigger(batch["id"])
        if existing:
            reports = [self.epoch_report(epoch["id"]) for epoch in existing]
            return {"reason": "existing", "reports": reports}

        corrections = self.corrected_periods(batch)
        if not corrections:
            # Nothing published was revised, so the reference baseline does not move.
            return {"reason": "no_published_value_revised", "reports": []}

        with self.store.transaction():
            indicators = sorted({item["indicator"] for item in corrections})
            epochs: list[dict[str, Any]] = []
            for indicator in indicators:
                series_corrections = [item for item in corrections if item["indicator"] == indicator]
                epoch = self.store.insert_baseline_epoch(
                    trigger_batch_id=batch["id"], trigger_revision=batch["revision"],
                    source=batch["source"], indicator=indicator,
                    corrected_periods=series_corrections,
                    idempotency_key=self.idempotency_key(batch) + ":" + indicator)
                self._propagate_to_series(epoch, batch, series_corrections)
                epochs.append(epoch)
        return {"reason": "opened", "reports": [self.epoch_report(epoch["id"]) for epoch in epochs]}

    def _propagate_to_series(self, epoch: dict[str, Any], trigger: dict[str, Any],
                             corrections: list[dict[str, Any]]) -> None:
        indicator = epoch["indicator"]
        contract = self.registry.indicator(indicator)
        earliest = min(period_index(item["period"], contract.cadence) for item in corrections)
        overrides = {item["period"]: item["new_value"] for item in corrections}
        for target in self.store.batches_for_series(trigger["source"], indicator):
            if target["id"] == trigger["id"]:
                continue
            if target["external_id"] == trigger["external_id"]:
                # Older revisions of the same delivery are superseded, not downstream.
                continue
            rows = [row for row in self.store.measurements(target["id"]) if row["indicator"] == indicator]
            if not any(period_index(row["period"], contract.cadence) > earliest for row in rows):
                continue
            self._reassess(epoch, target, rows, overrides)
        self.store.mark_baseline_epoch(epoch["id"], self._epoch_state(epoch["id"]))

    def _reassess(self, epoch: dict[str, Any], target: dict[str, Any],
                  rows: list[dict[str, Any]], overrides: dict[str, str]) -> None:
        prior_findings = list(target["findings"])
        measurements = [Measurement(target["source"], row["indicator"], row["period"], row["value"],
                                    row["unit"], row["raw_value"], row["raw_unit"], row["line"],
                                    row["factor"], row["offset"]) for row in rows]
        evidence = self.quality.jump_evidence_for_measurements(target["source"], measurements, overrides)
        retained = [item for item in prior_findings if item["code"] not in BASELINE_FINDING_CODES]
        refreshed = retained + [JumpDetector.evidence_finding(item).document() for item in evidence]
        refreshed.sort(key=lambda item: (item["severity"], item["code"], item["indicator"] or "",
                                         item["period"] or "", item["line"] or 0))
        is_published = bool(target["published_version"])
        if is_published:
            # A published batch belongs to an immutable release: it keeps its
            # lifecycle status, the refreshed verdict only adds review evidence.
            updated_status = target["status"]
        else:
            updated_status = self._status_from_findings(refreshed)
        needs_confirmation = bool(evidence)
        changed = updated_status != target["status"] or refreshed != prior_findings
        # Published batches are never rewritten; only non-published rows move.
        applied = changed and not is_published
        assessment = self.store.record_baseline_assessment(
            epoch_id=epoch["id"], batch_id=target["id"], prior_status=target["status"],
            updated_status=updated_status, prior_findings=prior_findings,
            updated_findings=refreshed, jump_evidence=evidence,
            needs_confirmation=needs_confirmation, applied=applied)
        if applied:
            self.store.mark_batch_baseline(target["id"], epoch["id"], updated_status, refreshed)
            self.store.event("baseline_rejudge", f"batch:{target['id']}",
                             {"epoch_id": epoch["id"], "prior_status": target["status"],
                              "updated_status": updated_status, "assessment_id": assessment["id"]})

    @staticmethod
    def _status_from_findings(findings: list[dict[str, Any]]) -> str:
        if any(item["severity"] == "block" for item in findings):
            return "rejected"
        if findings:
            return "review"
        return "accepted"

    # ------------------------------------------------------------------
    # Confirmation and reporting
    # ------------------------------------------------------------------
    def _epoch_state(self, epoch_id: int) -> str:
        pending = [item for item in self.store.baseline_assessments_for_epoch(epoch_id)
                   if item["needs_confirmation"] and item["confirmed_at"] is None]
        return "open" if pending else "confirmed"

    def confirm(self, assessment_id: int) -> dict[str, Any]:
        with self.store.transaction():
            assessment = self.store.baseline_assessment(assessment_id)
            if not assessment["needs_confirmation"]:
                raise ContractError("not_confirmable", "assessment does not require confirmation")
            if assessment["confirmed_at"] is None:
                self.store.confirm_baseline_assessment(assessment_id)
                self.store.event("baseline_confirm", f"assessment:{assessment_id}",
                                 {"epoch_id": assessment["epoch_id"], "batch_id": assessment["batch_id"]})
            epoch_id = assessment["epoch_id"]
            self.store.mark_baseline_epoch(epoch_id, self._epoch_state(epoch_id))
            return self.store.baseline_assessment(assessment_id)

    def pending_confirmations(self) -> list[dict[str, Any]]:
        pending = []
        for epoch in self.store.list_baseline_epochs():
            for assessment in self.store.baseline_assessments_for_epoch(epoch["id"]):
                if assessment["needs_confirmation"] and assessment["confirmed_at"] is None:
                    batch = self.store.batch(assessment["batch_id"])
                    pending.append({"assessment_id": assessment["id"], "epoch_id": epoch["id"],
                                    "batch_id": assessment["batch_id"], "source": batch["source"],
                                    "external_id": batch["external_id"], "revision": batch["revision"],
                                    "published_version": batch["published_version"],
                                    "updated_status": assessment["updated_status"],
                                    "jump_evidence": assessment["jump_evidence"]})
        return pending

    def epoch_report(self, epoch_id: int) -> dict[str, Any]:
        epoch = self.store.baseline_epoch(epoch_id)
        assessments = self.store.baseline_assessments_for_epoch(epoch_id)
        trigger = self.store.batch(epoch["trigger_batch_id"])
        return {"epoch": epoch, "trigger": {"batch_id": trigger["id"], "source": trigger["source"],
                                            "external_id": trigger["external_id"],
                                            "revision": trigger["revision"],
                                            "payload_hash": trigger["payload_hash"]},
                "assessments": assessments}

    def epochs(self) -> list[dict[str, Any]]:
        return self.store.list_baseline_epochs()

    def batch_baseline(self, batch_id: int) -> dict[str, Any]:
        """Explain the current baseline verdict of one batch."""
        batch = self.store.batch(batch_id)
        latest = self.store.latest_baseline_assessment(batch_id)
        return {"batch_id": batch_id, "baseline_epoch_id": batch["baseline_epoch_id"],
                "status": batch["status"],
                "latest_assessment": None if latest is None else {
                    "assessment_id": latest["id"], "epoch_id": latest["epoch_id"],
                    "prior_status": latest["prior_status"], "updated_status": latest["updated_status"],
                    "needs_confirmation": latest["needs_confirmation"],
                    "confirmed_at": latest["confirmed_at"], "applied": latest["applied"],
                    "jump_evidence": latest["jump_evidence"],
                    "updated_findings": latest["updated_findings"]}}

    def assert_release_ready(self, batch_id: int) -> None:
        """Publish guard: no stale baseline verdict may enter a new release."""
        unconfirmed = self.store.unconfirmed_assessments(batch_id)
        if unconfirmed:
            raise ContractError(
                "baseline_confirmation_pending",
                f"batch {batch_id} has {len(unconfirmed)} unconfirmed refreshed baseline verdict(s) "
                f"(latest epoch {unconfirmed[-1]['epoch_id']})")
        for epoch in self.store.epochs_for_trigger(batch_id):
            if epoch["status"] == "open":
                raise ContractError("baseline_confirmation_pending",
                                    f"correction batch {batch_id} opened baseline epoch {epoch['id']} "
                                    "with downstream confirmations still open")
