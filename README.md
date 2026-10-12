# Open Food Facts ingestion pipeline

A Python data-engineering project developed from an academic food-data case study. It validates product records, separates usable data from rejected inputs, and supports incremental loading into MySQL.

## What it demonstrates

- Data validation for protein values, nutrition scores, timestamps, and missing markers.
- Incremental API retrieval using a saved source timestamp.
- MySQL upserts that preserve existing values when later records omit them.
- Run tracking, rejection summaries, and an AWS Lambda entry point.
- An offline demonstration with fictional product fixtures and nine unit tests.

## Run the offline demo

Use Python 3.10 or newer. From the repository root, no third-party packages or credentials are needed:

```sh
python -m cloud_pipeline.lambda_function --sample cloud_pipeline/sample_products.json
python -m unittest discover -s cloud_pipeline/tests -v
```

The sample contains four products: two valid, two rejected, and one eligible for nutritional analysis. This is a validation demonstration, not a nutritional recommendation.

## How it works

```text
API or sample JSON -> validate and transform -> valid / rejected records
                                              |
                                   dry-run summary or MySQL upserts
                                              |
                                      run log and checkpoint
```

## Database and cloud integration

The offline demo is self-contained. Database mode is an integration component for the original case-study schema; this repository does not include that full database or claim a deployed cloud service.

`cloud_pipeline/database_updates.sql` targets the existing `sum26_foodfacts` database. It requires the original `products`, `product_scores`, `product_nutrients`, and `nutrients` tables and adds operational run/state tables. Review it before applying it to a database.

Database mode requires `pip install -r cloud_pipeline/requirements.txt` and the environment variables `DB_HOST`, `DB_PORT` (default 3306), `DB_NAME`, `DB_USER`, and `DB_PASSWORD`. Keep credentials outside Git. AWS deployment should use a managed secret store and appropriate permissions.

The Lambda handler is `cloud_pipeline.lambda_function.lambda_handler` when packaging this repository structure. EventBridge can schedule invocations. Example event:

```json
{"page_size": 50, "max_pages": 1}
```

For live API access, configure `OFF_USER_AGENT` with an application name and contact address. No live API data is bundled; the included sample is fictional.

## Scope and limitations

This is a small academic implementation, not a complete production ingestion platform. Automated checks exercise validation, dry runs, checkpoint handling, and repository interactions through fakes. They do not verify a live MySQL database or an AWS deployment. Same-timestamp updates, pagination boundaries, and recovery behavior deserve further review before production use.
