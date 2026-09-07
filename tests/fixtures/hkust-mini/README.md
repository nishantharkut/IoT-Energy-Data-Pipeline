# HKUST Mini Fixtures

This directory contains dynamically generated test fixtures for the IoT Energy Data Pipeline.

## Purpose

The `hkust-mini` fixtures are synthetic test data used for unit and integration testing. They are designed to cover the edge cases and data quality issues specified in the coursework design specification.

## Fixture Generation

All fixtures are generated dynamically by test factories. No source data from the HKUST smart-meter database is redistributed in this repository.

The fixture factories create:

- Three fictional energy meters
- Two fictional buildings
- 48 regular timestamps
- One null numeric cell
- One repeated meter timestamp
- One zero interval
- One unmatched Brick meter
- A separate malformed-TTL fixture

## Data Source

The real HKUST dataset is obtained separately from Dryad. See `docs/dataset.md` for download instructions. This fixture directory contains no copied dataset values.

## Test Usage

Tests use `tmp_path` fixtures and call factory functions to generate minimal `.xlsx` and `.ttl` files as needed. No fixture files are committed to version control.