"""Scheduled Open Food Facts ingestion for AWS Lambda or local dry runs.

The implementation intentionally stays small enough for the course assignment:
retrieve a limited recent batch, validate it, map it to the existing relational
schema, and use idempotent MySQL upserts.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

API_URL = "https://world.openfoodfacts.org/api/v2/search"
API_FIELDS = (
    "code,product_name,generic_name,quantity,ingredients_text,created_t,"
    "main_category,nutriscore_score,nutriscore_grade,nova_group,completeness,"
    "unique_scans_n,pnns_groups_1,pnns_groups_2,nutriments,last_modified_t"
)
PROTEIN_NUTRIENT_ID = 8
MISSING_MARKERS = {
    "unknown",
    "not-applicable",
    "not_applicable",
    "not-computed",
    "not_computed",
    "n/a",
    "na",
}


@dataclass(frozen=True)
class ProductRecord:
    product_id: str
    product_name: str | None
    generic_name: str | None
    quantity: str | None
    ingredients_text: str | None
    created_datetime: str | None
    main_category_tag: str | None
    nutriscore_score: int | None
    nutriscore_grade: str | None
    nova_group: int | None
    completeness: float | None
    unique_scans_n: int | None
    pnns_group_1: str | None
    pnns_group_2: str | None
    protein_100g: float | None
    source_last_modified_t: int
    analysis_eligible: bool


@dataclass(frozen=True)
class RejectedRecord:
    product_id: str | None
    reasons: list[str]


class Repository(Protocol):
    def get_checkpoint(self) -> int: ...

    def start_run(self) -> int: ...

    def upsert_records(self, records: Iterable[ProductRecord]) -> dict[str, int]: ...

    def finish_run(
        self,
        run_id: int,
        status: str,
        summary: dict[str, int],
        checkpoint: int,
        error_message: str | None = None,
    ) -> None: ...

    def close(self) -> None: ...


def _clean_text(value: Any, maximum: int | None = None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned[:maximum] if maximum else cleaned


def _optional_float(value: Any, field: str, reasons: list[str]) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in MISSING_MARKERS:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        reasons.append(f"{field} must be numeric")
        return None
    if not math.isfinite(number):
        reasons.append(f"{field} must be finite")
        return None
    return number


def _optional_int(value: Any, field: str, reasons: list[str]) -> int | None:
    number = _optional_float(value, field, reasons)
    if number is None:
        return None
    if not number.is_integer():
        reasons.append(f"{field} must be an integer")
        return None
    return int(number)


def _epoch_to_mysql(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        timestamp = int(value)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def validate_and_transform(
    product: dict[str, Any],
) -> tuple[ProductRecord | None, RejectedRecord | None]:
    """Validate one API object and map it to the course database schema."""

    reasons: list[str] = []
    product_id = _clean_text(product.get("code"), 64)
    if not product_id:
        reasons.append("product_id is required")
    elif len(str(product.get("code", "")).strip()) > 64:
        reasons.append("product_id exceeds 64 characters")

    nutriments = product.get("nutriments") or {}
    if not isinstance(nutriments, dict):
        reasons.append("nutriments must be an object")
        nutriments = {}

    protein = _optional_float(nutriments.get("proteins_100g"), "protein_100g", reasons)
    if protein is not None and not 0 <= protein <= 100:
        reasons.append("protein_100g must be between 0 and 100")

    grade = _clean_text(product.get("nutriscore_grade"), 32)
    grade = grade.lower() if grade else None
    if grade in MISSING_MARKERS:
        grade = None
    if grade is not None and grade not in set("abcde"):
        reasons.append("nutriscore_grade must be A-E or missing")

    score = _optional_int(product.get("nutriscore_score"), "nutriscore_score", reasons)
    nova = _optional_int(product.get("nova_group"), "nova_group", reasons)
    if nova == 0:
        nova = None
    if nova is not None and nova not in {1, 2, 3, 4}:
        reasons.append("nova_group must be 1-4 or missing")

    completeness = _optional_float(product.get("completeness"), "completeness", reasons)
    if completeness is not None and not 0 <= completeness <= 1:
        reasons.append("completeness must be between 0 and 1")
    scans = _optional_int(product.get("unique_scans_n"), "unique_scans_n", reasons)
    if scans is not None and scans < 0:
        reasons.append("unique_scans_n cannot be negative")

    modified = _optional_int(product.get("last_modified_t"), "last_modified_t", reasons)
    modified = modified or 0
    if modified < 0:
        reasons.append("last_modified_t cannot be negative")

    if reasons:
        return None, RejectedRecord(product_id=product_id, reasons=reasons)

    record = ProductRecord(
        product_id=product_id or "",
        product_name=_clean_text(product.get("product_name"), 512),
        generic_name=_clean_text(product.get("generic_name")),
        quantity=_clean_text(product.get("quantity"), 255),
        ingredients_text=_clean_text(product.get("ingredients_text")),
        created_datetime=_epoch_to_mysql(product.get("created_t")),
        main_category_tag=_clean_text(product.get("main_category"), 128),
        nutriscore_score=score,
        nutriscore_grade=grade,
        nova_group=nova,
        completeness=completeness,
        unique_scans_n=scans,
        pnns_group_1=_clean_text(product.get("pnns_groups_1"), 255),
        pnns_group_2=_clean_text(product.get("pnns_groups_2"), 255),
        protein_100g=protein,
        source_last_modified_t=modified,
        analysis_eligible=protein is not None and score is not None and grade is not None,
    )
    return record, None


def fetch_recent_products(
    since_timestamp: int = 0,
    page_size: int = 50,
    max_pages: int = 1,
    opener=urlopen,
    sleeper=time.sleep,
) -> list[dict[str, Any]]:
    """Fetch a small, newest-first API batch and stop at the checkpoint."""

    if not 1 <= page_size <= 100:
        raise ValueError("page_size must be between 1 and 100")
    if not 1 <= max_pages <= 5:
        raise ValueError("max_pages must be between 1 and 5")

    collected: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        query = urlencode(
            {
                "fields": API_FIELDS,
                "sort_by": "last_modified_t",
                "page": page,
                "page_size": page_size,
            }
        )
        request = Request(
            f"{os.getenv('OFF_API_URL', API_URL)}?{query}",
            headers={
                "User-Agent": os.getenv(
                    "OFF_USER_AGENT",
                    "UNF-OpenFoodFacts-CaseStudy/1.0 (academic project)",
                )
            },
        )
        payload = None
        for attempt in range(3):
            try:
                with opener(request, timeout=20) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise
            except URLError:
                if attempt == 2:
                    raise
            sleeper(2**attempt)
        if payload is None:
            raise RuntimeError("Open Food Facts request failed without a response")
        products = payload.get("products", [])
        if not isinstance(products, list):
            raise ValueError("Open Food Facts response did not contain a product list")

        reached_checkpoint = False
        for product in products:
            modified = product.get("last_modified_t") or 0
            try:
                modified = int(modified)
            except (TypeError, ValueError):
                modified = 0
            if since_timestamp and modified <= since_timestamp:
                reached_checkpoint = True
                continue
            collected.append(product)

        if reached_checkpoint or len(products) < page_size:
            break
    return collected


class MySQLRepository:
    """Small persistence adapter using the existing normalized MySQL tables."""

    def __init__(self, connection: Any):
        self.connection = connection

    @classmethod
    def from_environment(cls) -> "MySQLRepository":
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("Install PyMySQL before using database mode") from exc

        required = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise RuntimeError(f"Missing database settings: {', '.join(missing)}")
        connection = pymysql.connect(
            host=os.environ["DB_HOST"],
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_NAME"],
            charset="utf8mb4",
            autocommit=False,
        )
        return cls(connection)

    def get_checkpoint(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT state_value FROM pipeline_state WHERE state_key=%s",
                ("last_successful_source_timestamp",),
            )
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    def start_run(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (started_at, status) VALUES (UTC_TIMESTAMP(), %s)",
                ("RUNNING",),
            )
            run_id = int(cursor.lastrowid)
        self.connection.commit()
        return run_id

    def upsert_records(self, records: Iterable[ProductRecord]) -> dict[str, int]:
        counts = {"inserted_or_updated": 0, "protein_values_loaded": 0}
        try:
            with self.connection.cursor() as cursor:
                for record in records:
                    cursor.execute(
                        """
                        INSERT INTO products
                            (product_id, product_name, generic_name, quantity,
                             ingredients_text, created_datetime, main_category_tag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            product_name=COALESCE(VALUES(product_name), product_name),
                            generic_name=COALESCE(VALUES(generic_name), generic_name),
                            quantity=COALESCE(VALUES(quantity), quantity),
                            ingredients_text=COALESCE(VALUES(ingredients_text), ingredients_text),
                            created_datetime=COALESCE(VALUES(created_datetime), created_datetime),
                            main_category_tag=COALESCE(VALUES(main_category_tag), main_category_tag)
                        """,
                        (
                            record.product_id,
                            record.product_name,
                            record.generic_name,
                            record.quantity,
                            record.ingredients_text,
                            record.created_datetime,
                            record.main_category_tag,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO product_scores
                            (product_id, nutriscore_score, nutriscore_grade, nova_group,
                             completeness, unique_scans_n, pnns_group_1, pnns_group_2)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            nutriscore_score=COALESCE(VALUES(nutriscore_score), nutriscore_score),
                            nutriscore_grade=COALESCE(VALUES(nutriscore_grade), nutriscore_grade),
                            nova_group=COALESCE(VALUES(nova_group), nova_group),
                            completeness=COALESCE(VALUES(completeness), completeness),
                            unique_scans_n=COALESCE(VALUES(unique_scans_n), unique_scans_n),
                            pnns_group_1=COALESCE(VALUES(pnns_group_1), pnns_group_1),
                            pnns_group_2=COALESCE(VALUES(pnns_group_2), pnns_group_2)
                        """,
                        (
                            record.product_id,
                            record.nutriscore_score,
                            record.nutriscore_grade,
                            record.nova_group,
                            record.completeness,
                            record.unique_scans_n,
                            record.pnns_group_1,
                            record.pnns_group_2,
                        ),
                    )
                    if record.protein_100g is not None:
                        cursor.execute(
                            """
                            INSERT INTO product_nutrients
                                (product_id, nutrient_id, value_100g)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE value_100g=VALUES(value_100g)
                            """,
                            (
                                record.product_id,
                                PROTEIN_NUTRIENT_ID,
                                record.protein_100g,
                            ),
                        )
                        counts["protein_values_loaded"] += 1
                    counts["inserted_or_updated"] += 1
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return counts

    def finish_run(
        self,
        run_id: int,
        status: str,
        summary: dict[str, int],
        checkpoint: int,
        error_message: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_runs
                SET completed_at=UTC_TIMESTAMP(), status=%s, records_received=%s,
                    records_valid=%s, records_rejected=%s, error_message=%s
                WHERE run_id=%s
                """,
                (
                    status,
                    summary.get("products_received", 0),
                    summary.get("products_valid", 0),
                    summary.get("products_rejected", 0),
                    error_message,
                    run_id,
                ),
            )
            if status == "SUCCEEDED":
                cursor.execute(
                    """
                    INSERT INTO pipeline_state (state_key, state_value, updated_at)
                    VALUES (%s, %s, UTC_TIMESTAMP())
                    ON DUPLICATE KEY UPDATE
                        state_value=VALUES(state_value), updated_at=VALUES(updated_at)
                    """,
                    ("last_successful_source_timestamp", str(checkpoint)),
                )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def run_pipeline(
    products: list[dict[str, Any]],
    repository: Repository | None = None,
    checkpoint: int = 0,
) -> dict[str, Any]:
    valid: list[ProductRecord] = []
    rejected: list[RejectedRecord] = []
    for product in products:
        record, rejection = validate_and_transform(product)
        if record:
            valid.append(record)
        elif rejection:
            rejected.append(rejection)

    received_timestamps = []
    for product in products:
        try:
            received_timestamps.append(int(product.get("last_modified_t") or 0))
        except (TypeError, ValueError):
            continue
    new_checkpoint = max([checkpoint, *received_timestamps])
    summary: dict[str, Any] = {
        "products_received": len(products),
        "products_valid": len(valid),
        "products_rejected": len(rejected),
        "analysis_eligible": sum(record.analysis_eligible for record in valid),
        "checkpoint": new_checkpoint,
        "rejections": [asdict(item) for item in rejected],
    }
    if repository is not None:
        summary.update(repository.upsert_records(valid))
    LOGGER.info(json.dumps({"event": "pipeline_summary", **summary}, default=str))
    return summary


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    """AWS Lambda entry point; pass ``dry_run=true`` for a database-free test."""

    event = event or {}
    dry_run = bool(event.get("dry_run", False))
    repository: MySQLRepository | None = None
    run_id: int | None = None
    checkpoint = int(event.get("checkpoint", 0))
    try:
        if not dry_run:
            repository = MySQLRepository.from_environment()
            checkpoint = repository.get_checkpoint()
            run_id = repository.start_run()

        products = event.get("products")
        if products is None:
            products = fetch_recent_products(
                since_timestamp=checkpoint,
                page_size=int(event.get("page_size", os.getenv("OFF_PAGE_SIZE", "50"))),
                max_pages=int(event.get("max_pages", os.getenv("OFF_MAX_PAGES", "1"))),
            )
        if not isinstance(products, list):
            raise ValueError("event.products must be a list")

        summary = run_pipeline(products, repository=repository, checkpoint=checkpoint)
        if repository is not None and run_id is not None:
            repository.finish_run(run_id, "SUCCEEDED", summary, summary["checkpoint"])
        return {"statusCode": 200, "body": json.dumps(summary, default=str)}
    except Exception as exc:
        LOGGER.exception("Pipeline failed")
        if repository is not None and run_id is not None:
            failed = {"products_received": 0, "products_valid": 0, "products_rejected": 0}
            repository.finish_run(run_id, "FAILED", failed, checkpoint, str(exc)[:2000])
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}
    finally:
        if repository is not None:
            repository.close()


def _main() -> int:
    parser = argparse.ArgumentParser(description="Run the cloud pipeline locally")
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--checkpoint", type=int, default=0)
    args = parser.parse_args()
    products = json.loads(args.sample.read_text(encoding="utf-8"))
    response = lambda_handler(
        {"dry_run": True, "checkpoint": args.checkpoint, "products": products}, None
    )
    print(json.dumps(json.loads(response["body"]), indent=2))
    return 0 if response["statusCode"] == 200 else 1


if __name__ == "__main__":
    sys.exit(_main())
