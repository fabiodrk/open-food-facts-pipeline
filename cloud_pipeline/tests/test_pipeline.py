from __future__ import annotations

import json
import unittest

from cloud_pipeline.lambda_function import (
    fetch_recent_products,
    lambda_handler,
    run_pipeline,
    validate_and_transform,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class PipelineTests(unittest.TestCase):
    def test_valid_product_is_mapped_and_analysis_eligible(self):
        product = {
            "code": "123",
            "product_name": "Sample",
            "nutriscore_score": 1,
            "nutriscore_grade": "B",
            "nova_group": 3,
            "last_modified_t": 200,
            "nutriments": {"proteins_100g": "14.5"},
        }
        record, rejected = validate_and_transform(product)
        self.assertIsNone(rejected)
        self.assertEqual(record.product_id, "123")
        self.assertEqual(record.nutriscore_grade, "b")
        self.assertEqual(record.protein_100g, 14.5)
        self.assertTrue(record.analysis_eligible)

    def test_missing_score_is_valid_but_not_analysis_eligible(self):
        record, rejected = validate_and_transform(
            {
                "code": "124",
                "last_modified_t": 201,
                "nutriments": {"proteins_100g": 3},
            }
        )
        self.assertIsNone(rejected)
        self.assertFalse(record.analysis_eligible)

    def test_impossible_protein_is_rejected(self):
        record, rejected = validate_and_transform(
            {
                "code": "125",
                "nutriscore_grade": "a",
                "nutriments": {"proteins_100g": 101},
            }
        )
        self.assertIsNone(record)
        self.assertIn("protein_100g must be between 0 and 100", rejected.reasons)

    def test_invalid_grade_is_rejected(self):
        record, rejected = validate_and_transform(
            {"code": "126", "nutriscore_grade": "Z", "nutriments": {}}
        )
        self.assertIsNone(record)
        self.assertIn("nutriscore_grade must be A-E or missing", rejected.reasons)

    def test_source_missing_markers_are_retained_as_missing(self):
        record, rejected = validate_and_transform(
            {
                "code": "126b",
                "nutriscore_score": "unknown",
                "nutriscore_grade": "not-applicable",
                "nova_group": 0,
                "nutriments": {"proteins_100g": 6},
            }
        )
        self.assertIsNone(rejected)
        self.assertIsNone(record.nutriscore_score)
        self.assertIsNone(record.nutriscore_grade)
        self.assertIsNone(record.nova_group)
        self.assertFalse(record.analysis_eligible)

    def test_dry_run_summary(self):
        products = [
            {
                "code": "127",
                "nutriscore_score": 0,
                "nutriscore_grade": "a",
                "last_modified_t": 300,
                "nutriments": {"proteins_100g": 13},
            },
            {"code": "128", "nutriments": {"proteins_100g": 150}},
        ]
        response = lambda_handler({"dry_run": True, "products": products}, None)
        body = json.loads(response["body"])
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(body["products_received"], 2)
        self.assertEqual(body["products_valid"], 1)
        self.assertEqual(body["products_rejected"], 1)
        self.assertEqual(body["analysis_eligible"], 1)
        self.assertEqual(body["checkpoint"], 300)

    def test_rejected_newest_record_advances_checkpoint(self):
        summary = run_pipeline(
            [
                {
                    "code": "valid",
                    "last_modified_t": 400,
                    "nutriments": {"proteins_100g": 10},
                },
                {
                    "code": "invalid",
                    "last_modified_t": 500,
                    "nutriments": {"proteins_100g": 150},
                },
            ],
            checkpoint=300,
        )
        self.assertEqual(summary["checkpoint"], 500)

    def test_fetch_stops_at_checkpoint(self):
        calls = []

        def opener(request, timeout):
            calls.append(request.full_url)
            return FakeResponse(
                {
                    "products": [
                        {"code": "new", "last_modified_t": 500},
                        {"code": "old", "last_modified_t": 400},
                    ]
                }
            )

        products = fetch_recent_products(
            since_timestamp=450,
            page_size=2,
            max_pages=3,
            opener=opener,
            sleeper=lambda seconds: None,
        )
        self.assertEqual([item["code"] for item in products], ["new"])
        self.assertEqual(len(calls), 1)
        self.assertIn("sort_by=last_modified_t", calls[0])

    def test_repository_receives_only_valid_records(self):
        class FakeRepository:
            def __init__(self):
                self.records = []

            def upsert_records(self, records):
                self.records = list(records)
                return {
                    "inserted_or_updated": len(self.records),
                    "protein_values_loaded": sum(
                        record.protein_100g is not None for record in self.records
                    ),
                }

        repository = FakeRepository()
        summary = run_pipeline(
            [
                {"code": "valid", "nutriments": {"proteins_100g": 5}},
                {"code": "invalid", "nutriments": {"proteins_100g": 500}},
            ],
            repository=repository,
        )
        self.assertEqual([record.product_id for record in repository.records], ["valid"])
        self.assertEqual(summary["inserted_or_updated"], 1)
        self.assertEqual(summary["protein_values_loaded"], 1)


if __name__ == "__main__":
    unittest.main()
