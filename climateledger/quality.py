from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Any

from .normalize import Measurement, index_by_series, period_index, period_from_index
from .registry import Registry


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    indicator: str | None
    period: str | None
    line: int | None
    detail: str

    def document(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Assessment:
    status: str
    accepted_count: int
    rejected_count: int
    findings: tuple[Finding, ...]

    def document(self) -> dict[str, Any]:
        return {"status": self.status, "accepted_count": self.accepted_count,
                "rejected_count": self.rejected_count,
                "findings": [item.document() for item in self.findings]}


class JumpDetector:
    """Explainable level-shift test against a historical reference window.

    A measurement is a jump when either its z score against the reference
    window exceeds ``sigma_threshold`` or its absolute deviation from the
    reference mean reaches the optional ``min_delta`` floor. The two tests are
    OR-combined because the sample standard deviation of a two-point window is
    always its half-range, which caps any z score below sqrt(2) and would make
    a sigma-only rule blind on small windows. A single-point reference uses the
    absolute deviation as its score; a constant reference flags any different
    value without dividing by zero. Every decision keeps the numbers it used so
    the verdict can be reproduced and inspected later.
    """

    def __init__(self, registry: Registry, store):
        self.registry = registry
        self.store = store

    @staticmethod
    def mean(values: list[Decimal]) -> Decimal:
        return sum(values, Decimal(0)) / Decimal(len(values))

    @staticmethod
    def sample_stdev(values: list[Decimal]) -> Decimal:
        average = JumpDetector.mean(values)
        variance = sum((item - average) ** 2 for item in values) / Decimal(len(values) - 1)
        return variance.sqrt()

    def rule(self, indicator: str) -> dict[str, Any] | None:
        return self.store.get_anomaly_rule(indicator)

    def explain(self, indicator: str, period: str, value: Decimal,
                reference: list[Decimal]) -> dict[str, Any] | None:
        rule = self.rule(indicator)
        if rule is None or len(reference) < rule["min_baseline_points"]:
            return None
        mean = self.mean(reference)
        deviation = abs(value - mean)
        min_delta = Decimal(rule["min_delta"]) if rule.get("min_delta") is not None else None
        if len(reference) >= 2:
            spread = self.sample_stdev(reference)
            if spread == 0:
                # Deterministic reference: any different value is a jump, an
                # identical value never is. Avoid a zero-division verdict.
                is_jump = value != mean
                z_score = None
            else:
                z_score = deviation / spread
                # The sample stdev of a two-point window always equals the
                # half-range, capping its z score below sqrt(2); the configurable
                # min_delta keeps small windows meaningful.
                sigma_hit = z_score >= Decimal(rule["sigma_threshold"])
                delta_hit = min_delta is not None and deviation >= min_delta
                is_jump = sigma_hit or delta_hit
        else:
            spread = None
            z_score = deviation
            sigma_hit = deviation >= Decimal(rule["sigma_threshold"])
            delta_hit = min_delta is not None and deviation >= min_delta
            is_jump = sigma_hit or delta_hit
        return {"indicator": indicator, "period": period, "value": str(value),
                "reference_points": len(reference), "reference_mean": str(mean),
                "reference_stdev": str(spread) if spread is not None else None,
                "sigma_threshold": rule["sigma_threshold"],
                "min_delta": rule.get("min_delta"),
                "deviation": str(deviation),
                "score": None if z_score is None else str(z_score),
                "is_jump": bool(is_jump)}

    @staticmethod
    def evidence_finding(evidence: dict[str, Any]) -> Finding:
        if evidence["reference_stdev"] is None:
            basis = f"single reference mean {evidence['reference_mean']}, deviation {evidence['deviation']}"
        elif evidence["score"] is None:
            basis = f"constant reference mean {evidence['reference_mean']}"
        else:
            basis = (f"reference mean {evidence['reference_mean']} stdev {evidence['reference_stdev']}, "
                     f"score {evidence['score']} vs {evidence['sigma_threshold']}, "
                     f"deviation {evidence['deviation']}"
                     + (f" vs min_delta {evidence['min_delta']}" if evidence["min_delta"] else ""))
        return Finding("review", "baseline_jump", evidence["indicator"], evidence["period"], None,
                       f"level shift against reference baseline: {basis}")


class QualityGate:
    def __init__(self, registry: Registry, store):
        self.registry = registry
        self.store = store
        self.jumps = JumpDetector(registry, store)

    def reference_window(self, source: str, indicator: str, before_index: int,
                         overrides: dict[str, str] | None = None) -> list[Decimal]:
        """Published values of a series strictly before a period.

        ``overrides`` maps a period to a hypothetical corrected value so the
        same routine can re-judge downstream batches against a baseline that
        includes a correction which has not been published yet.
        """
        values: list[Decimal] = []
        latest = self.store.latest_release()
        if latest is None:
            return values
        for entry in self.store.release_entries(latest["version"]):
            if entry["source"] != source or entry["indicator"] != indicator:
                continue
            if period_index(entry["period"], self.registry.indicator(indicator).cadence) >= before_index:
                continue
            value = overrides.get(entry["period"], entry["value"]) if overrides else entry["value"]
            values.append(Decimal(value))
        return values

    def jump_evidence_for_measurements(
            self, source: str, measurements: list[Measurement],
            overrides: dict[str, str] | None = None) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for item in measurements:
            indicator = self.registry.indicator(item.indicator)
            before_index = period_index(item.period, indicator.cadence)
            reference = self.reference_window(source, item.indicator, before_index, overrides)
            explanation = self.jumps.explain(item.indicator, item.period, Decimal(item.value), reference)
            if explanation is not None and explanation["is_jump"]:
                evidence.append(explanation)
        return evidence

    def assess(self, source_key: str, measurements: list[Measurement],
               rejected: list[dict[str, Any]]) -> Assessment:
        findings: list[Finding] = []
        for item in rejected:
            findings.append(Finding("block", item["code"], None, None, item["line"], item["detail"]))
        series = index_by_series(measurements)
        seen: dict[tuple[str, str], Measurement] = {}
        for item in measurements:
            key = item.indicator, item.period
            previous = seen.get(key)
            if previous:
                code = "duplicate_period" if previous.value == item.value else "conflicting_period"
                findings.append(Finding("block", code, item.indicator, item.period, item.line,
                                        f"period previously supplied on line {previous.line}"))
            else:
                seen[key] = item
            indicator = self.registry.indicator(item.indicator)
            number = Decimal(item.value)
            if not indicator.allow_negative and number < 0:
                findings.append(Finding("block", "negative_value", item.indicator, item.period,
                                        item.line, "negative measurement not allowed"))
            if indicator.minimum is not None and number < Decimal(indicator.minimum):
                findings.append(Finding("block", "below_minimum", item.indicator, item.period,
                                        item.line, f"below registered minimum {indicator.minimum}"))
            if indicator.maximum is not None and number > Decimal(indicator.maximum):
                findings.append(Finding("block", "above_maximum", item.indicator, item.period,
                                        item.line, f"above registered maximum {indicator.maximum}"))
        for key, items in series.items():
            indicator = self.registry.indicator(key)
            indices = sorted({period_index(item.period, indicator.cadence) for item in items})
            if indices:
                missing = sorted(set(range(indices[0], indices[-1] + 1)) - set(indices))
                if len(missing) > 1000:
                    findings.append(Finding("block", "coverage_span", key, None, None,
                                            "more than 1000 missing periods"))
                else:
                    for index in missing:
                        period = period_from_index(index, indicator.cadence)
                        findings.append(Finding("review", "missing_period", key, period, None,
                                                "gap within submitted series"))
            for item in items:
                prior = self.store.current_measurement(source_key, key, item.period)
                if prior is not None and prior["value"] != item.value:
                    findings.append(Finding("review", "revised_value", key, item.period, item.line,
                                            f"published value {prior['value']} differs"))
        for explanation in self.jump_evidence_for_measurements(source_key, measurements):
            findings.append(JumpDetector.evidence_finding(explanation))
        if not measurements:
            findings.append(Finding("block", "empty_batch", None, None, None, "no accepted measurements"))
        status = "rejected" if any(item.severity == "block" for item in findings) else (
            "review" if findings else "accepted")
        findings.sort(key=lambda item: (item.severity, item.code, item.indicator or "", item.period or "", item.line or 0))
        return Assessment(status, len(measurements), len(rejected), tuple(findings))

    def reviewable(self, assessment: Assessment) -> list[Finding]:
        return [finding for finding in assessment.findings if finding.severity == "review"]

    def blocking(self, assessment: Assessment) -> list[Finding]:
        return [finding for finding in assessment.findings if finding.severity == "block"]
