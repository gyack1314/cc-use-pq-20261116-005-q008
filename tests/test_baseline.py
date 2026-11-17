from __future__ import annotations

from climateledger.registry import ContractError
from support import LedgerCase


RULE = {"indicator": "co2_ppm", "sigma_threshold": "2.0",
        "min_baseline_points": 2, "min_delta": "3.0"}


class BaselineRuleTests(LedgerCase):
    def test_register_and_list_anomaly_rule(self):
        rule = self.service.register_anomaly_rule(RULE)
        self.assertEqual(rule["sigma_threshold"], "2.0")
        self.assertEqual(rule["min_baseline_points"], 2)
        self.assertEqual(rule["min_delta"], "3.0")
        self.assertEqual(self.service.baseline.rules()[0]["indicator"], "co2_ppm")

    def test_invalid_sigma_rejected(self):
        with self.assertRaisesRegex(ContractError, "anomaly_rule"):
            self.service.register_anomaly_rule({**RULE, "sigma_threshold": "0"})

    def test_invalid_min_delta_rejected(self):
        with self.assertRaisesRegex(ContractError, "anomaly_rule"):
            self.service.register_anomaly_rule({**RULE, "min_delta": "-1"})

    def test_unknown_indicator_rule_rejected(self):
        with self.assertRaisesRegex(ContractError, "unknown_indicator"):
            self.service.register_anomaly_rule({**RULE, "indicator": "missing"})


class BaselineJumpTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.service.register_anomaly_rule(RULE)

    def test_stable_value_does_not_jump(self):
        batch, _ = self.ingest("stable", self.rows(428, 429, 430))
        self.assertEqual(batch["status"], "accepted")
        self.assertNotIn("baseline_jump", {f["code"] for f in batch["findings"]})

    def test_spike_against_published_baseline_flags_review(self):
        first, _ = self.ingest("base", self.rows(428, 429))
        self.service.publish(first["id"], first["revision"])
        # Deviation from the mean (428.5) is 6.5, well beyond min_delta 3.0.
        spike, _ = self.ingest("spike", self.rows(435, start=3))
        self.assertEqual(spike["status"], "review")
        finding = next(f for f in spike["findings"] if f["code"] == "baseline_jump")
        self.assertEqual(finding["period"], "2026-03")
        self.assertIn("reference mean", finding["detail"])

    def test_jump_finding_carries_explainable_evidence(self):
        first, _ = self.ingest("base", self.rows(428, 429))
        self.service.publish(first["id"], first["revision"])
        spike, _ = self.ingest("spike", self.rows(435, start=3))
        finding = next(f for f in spike["findings"] if f["code"] == "baseline_jump")
        self.assertIn("min_delta", finding["detail"])


class CorrectionPropagationTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.service.register_anomaly_rule(RULE)

    def build_series(self):
        # Constant Jan/Feb baseline; March matches the mean exactly, so no jump.
        first, _ = self.ingest("months_1_2", self.rows(402, 402))
        self.service.publish(first["id"], first["revision"])
        later, _ = self.ingest("month_3", self.rows(402, start=3))
        self.service.publish(later["id"], later["revision"])
        return first, later

    def test_correction_opens_epoch_and_rejudges_downstream(self):
        _, later = self.build_series()
        # Correct January to 409: baseline shifts to (409, 402) and the March
        # reading 402 deviates by more than min_delta, becoming a jump.
        correction, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.assertEqual(correction["revision"], 2)
        self.assertEqual(correction["status"], "review")
        self.service.approve(correction["id"], 2)

        epochs = self.service.baseline_epochs()
        self.assertEqual(len(epochs), 1)
        epoch = self.service.baseline_epoch(epochs[0]["id"])
        self.assertEqual(epoch["epoch"]["indicator"], "co2_ppm")
        self.assertEqual(epoch["epoch"]["corrected_periods"][0]["period"], "2026-01")
        self.assertEqual(epoch["epoch"]["corrected_periods"][0]["old_value"], "402.00")
        self.assertEqual(epoch["epoch"]["corrected_periods"][0]["new_value"], "409.00")

        # The downstream March batch is the only re-assessed batch.
        assessed = epoch["assessments"]
        self.assertEqual([row["batch_id"] for row in assessed], [later["id"]])
        downstream = assessed[0]
        self.assertTrue(downstream["needs_confirmation"])
        self.assertEqual(downstream["updated_status"], "published")
        self.assertIn("baseline_jump", {f["code"] for f in downstream["updated_findings"]})
        evidence = downstream["jump_evidence"][0]
        self.assertEqual(evidence["period"], "2026-03")
        self.assertEqual(evidence["reference_points"], 2)
        self.assertEqual(evidence["deviation"], "3.50")

    def test_old_snapshot_and_rationale_remain_queryable(self):
        self.build_series()
        correction, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(correction["id"], 2)
        # The already-published downstream batch keeps its original status and
        # findings on the immutable batch row.
        later_history = self.service.batch_history("lab_a", "month_3")
        original = next(row for row in later_history if row["revision"] == 1)
        stored = self.service.batch(original["batch_id"])
        self.assertEqual(stored["status"], "published")
        self.assertNotIn("baseline_jump", {f["code"] for f in stored["findings"]})
        # But the refreshed verdict and its evidence are retrievable.
        baseline = self.service.batch_baseline(stored["id"])
        self.assertEqual(baseline["latest_assessment"]["updated_status"], "published")
        self.assertTrue(baseline["latest_assessment"]["needs_confirmation"])
        # And the old release still exports and verifies independently.
        self.assertTrue(self.service.verify(self.service.export(2)))

    def test_publish_blocked_until_baseline_confirmed(self):
        self.build_series()
        correction, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(correction["id"], 2)
        # The correction itself cannot publish while its epoch has open work.
        with self.assertRaisesRegex(ContractError, "baseline_confirmation_pending"):
            self.service.publish(correction["id"], 2)

        pending = self.service.baseline_pending()
        self.assertEqual(len(pending), 1)
        assessment_id = pending[0]["assessment_id"]
        self.service.confirm_baseline(assessment_id)
        self.assertEqual(self.service.baseline_pending(), [])
        # Once confirmed, the correction publishes and the corrected value ships.
        release = self.service.publish(correction["id"], 2)
        january = next(row for row in self.service.export(release["version"])["entries"]
                       if row["period"] == "2026-01")
        self.assertEqual(january["value"], "409.00")

    def test_repeated_correction_delivery_does_not_open_new_epoch(self):
        self.build_series()
        correction, manifest = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(correction["id"], 2)
        first_epochs = self.service.baseline_epochs()
        retried = self.service.ingest(manifest)
        self.assertTrue(retried["idempotent"])
        self.assertEqual(self.service.baseline_epochs(), first_epochs)
        self.assertEqual(len(self.service.baseline_epoch(first_epochs[0]["id"])["assessments"]), 1)

    def test_interrupted_retry_is_idempotent(self):
        self.build_series()
        correction, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(correction["id"], 2)
        epoch_id = self.service.baseline_epochs()[0]["id"]
        # Re-running propagation directly must reuse the existing epoch.
        again = self.service.baseline.propagate(self.service.batch(correction["id"]))
        self.assertEqual(again["reason"], "existing")
        self.assertEqual(len(self.service.baseline_epochs()), 1)
        self.assertEqual(self.service.baseline_epoch(epoch_id)["epoch"]["id"], epoch_id)

    def test_benign_correction_keeps_downstream_publishable(self):
        # January corrected to 403: the March reading 402 stays within min_delta
        # of the shifted baseline, so no new jump and no confirmation is needed.
        first, _ = self.ingest("months_1_2", self.rows(402, 402))
        self.service.publish(first["id"], first["revision"])
        later, _ = self.ingest("month_3", self.rows(402, start=3))
        self.service.publish(later["id"], later["revision"])
        correction, _ = self.ingest("months_1_2", self.rows(403, 402))
        self.service.approve(correction["id"], correction["revision"])
        assessment = self.service.baseline_epoch(self.service.baseline_epochs()[0]["id"])["assessments"][0]
        self.assertFalse(assessment["needs_confirmation"])
        self.assertEqual(assessment["updated_status"], "published")
        # No open confirmations, so the correction can publish straight away.
        release = self.service.publish(correction["id"], correction["revision"])
        self.assertEqual(release["version"], 3)

    def test_revision_chain_batches_are_not_treated_as_downstream(self):
        self.build_series()
        # A second revision of the same delivery must not re-judge itself.
        self.ingest("months_1_2", self.rows(409, 402))
        epoch = self.service.baseline_epoch(self.service.baseline_epochs()[0]["id"])
        batch_ids = {row["batch_id"] for row in epoch["assessments"]}
        trigger_id = epoch["trigger"]["batch_id"]
        self.assertNotIn(trigger_id, batch_ids)

    def test_confirmation_is_idempotent(self):
        self.build_series()
        correction, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(correction["id"], 2)
        assessment_id = self.service.baseline_pending()[0]["assessment_id"]
        first = self.service.confirm_baseline(assessment_id)
        second = self.service.confirm_baseline(assessment_id)
        self.assertEqual(first["confirmed_at"], second["confirmed_at"])
        self.assertEqual(self.service.baseline_epochs()[0]["status"], "confirmed")

    def test_confirming_non_required_assessment_rejected(self):
        first, _ = self.ingest("base", self.rows(402, 402))
        self.service.publish(first["id"], first["revision"])
        later, _ = self.ingest("month_3", self.rows(402, start=3))
        self.service.publish(later["id"], later["revision"])
        correction, _ = self.ingest("base", self.rows(403, 402))
        self.service.approve(correction["id"], correction["revision"])
        assessment_id = self.service.baseline_epoch(self.service.baseline_epochs()[0]["id"])["assessments"][0]["id"]
        with self.assertRaisesRegex(ContractError, "not_confirmable"):
            self.service.confirm_baseline(assessment_id)

    def test_review_queue_lists_rejudged_unpublished_batch(self):
        # A downstream batch that was never published gets re-judged in place.
        first, _ = self.ingest("base", self.rows(402, 402))
        self.service.publish(first["id"], first["revision"])
        later, _ = self.ingest("later", self.rows(402, start=3))
        self.assertEqual(later["status"], "accepted")
        correction, _ = self.ingest("base", self.rows(409, 402))
        self.service.approve(correction["id"], correction["revision"])
        rejudged = self.service.batch(later["id"])
        self.assertEqual(rejudged["status"], "review")
        self.assertIn("baseline_jump", {f["code"] for f in rejudged["findings"]})
        pending_ids = {item["batch_id"] for item in self.service.pending_review()}
        self.assertIn(later["id"], pending_ids)

    def test_no_correction_means_no_epoch(self):
        first, _ = self.ingest("base", self.rows(402, 402))
        self.service.publish(first["id"], first["revision"])
        # Same values re-delivered: idempotent, nothing revised, no epoch.
        result = self.ingest("base", self.rows(402, 402))
        self.assertEqual(result[0]["status"], "published")
        self.assertEqual(self.service.baseline_epochs(), [])

    def test_second_correction_round_blocks_publish_until_confirmed(self):
        self.build_series()
        # First correction round: January to 409 makes March a jump; confirm it.
        first_corr, _ = self.ingest("months_1_2", self.rows(409, 402))
        self.service.approve(first_corr["id"], first_corr["revision"])
        first_aid = self.service.baseline_pending()[0]["assessment_id"]
        self.service.confirm_baseline(first_aid)
        self.service.publish(first_corr["id"], first_corr["revision"])
        self.assertEqual(len(self.service.baseline_epochs()), 1)

        # Second, further correction: January to 412 opens another epoch and
        # re-judges the same downstream March batch again.
        second_corr, _ = self.ingest("months_1_2", self.rows(412, 402))
        self.service.approve(second_corr["id"], second_corr["revision"])
        self.assertEqual(len(self.service.baseline_epochs()), 2)
        march = next(b for b in self.service.batches() if b["external_id"] == "month_3")
        # Two assessments now exist for the March batch; the latest is unconfirmed.
        self.assertEqual(len(self.service.store.unconfirmed_assessments(march["id"])), 1)
        with self.assertRaisesRegex(ContractError, "baseline_confirmation_pending"):
            self.service.publish(second_corr["id"], second_corr["revision"])
        second_aid = self.service.baseline_pending()[0]["assessment_id"]
        self.service.confirm_baseline(second_aid)
        release = self.service.publish(second_corr["id"], second_corr["revision"])
        self.assertEqual(release["version"], 4)
        # Both rounds and both rationales remain queryable.
        history = self.service.batch_baseline(march["id"])
        self.assertEqual(history["latest_assessment"]["epoch_id"], 2)
        self.assertEqual(len(self.service.baseline_epoch(1)["assessments"]), 1)
        self.assertEqual(len(self.service.baseline_epoch(2)["assessments"]), 1)
