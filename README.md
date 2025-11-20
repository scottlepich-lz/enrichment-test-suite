# Enrichment Test Suite

Test suite for validating the LegalZoom Experimentation API enrichment endpoint.

## Purpose

This suite tests the enrichment endpoint by:
1. Removing LTV/COGS values from source events
2. Sending cleaned events to the dev enrichment API
3. Validating that responses contain properly calculated LTV, COGS, and Net LTV
4. Reporting PASS/FAIL results for each event

## Files

### Script
- **`enrich_csv_with_test.py`** - Main enrichment test script

### Sample Results
- **`order-complete-enriched-20251119_173455.csv`** - Test results from 1000 events
  - 100% success rate (1000 PASSED, 0 FAILED)
  - Total LTV: $296,685.31
  - Total COGS: $57,965.01
  - Average processing rate: 4.9 events/sec

## Input CSV Format

The input CSV must have these columns:

```csv
EVENT_ID,FULL_EVENT_PAYLOAD
ajs-next-1763470936292-...,"{\"event\":\"Order Completed\",\"properties\":{...}}"
```

- **EVENT_ID**: Unique identifier for the event
- **FULL_EVENT_PAYLOAD**: JSON string containing the complete Segment event

### Getting Test Events from Snowflake

Use the provided SQL query to extract events:

```bash
# Run the query in Snowflake
snowsql -f get-test-events.sql -o output_format=csv > order-complete.csv

# Or manually:
# 1. Open get-test-events.sql
# 2. Run query in Snowflake web UI
# 3. Export results as CSV
# 4. Save as order-complete.csv
```

The query (`get-test-events.sql`) extracts:
- Last 7 days of Order Completed events
- Limited to 1000 events by default
- From: `analytics.product.fct_segment_order_completed_event`

## Output CSV Format

The output CSV contains:

```csv
EVENT_ID,TEST_RESULT,FULL_EVENT_PAYLOAD,ENRICHED_RESPONSE
ajs-next-...,PASS,"{...original...}","{...enriched...}"
```

- **EVENT_ID**: Event identifier (copied from input)
- **TEST_RESULT**: `PASS` or `FAIL`
- **FULL_EVENT_PAYLOAD**: Original event (for reference/comparison)
- **ENRICHED_RESPONSE**: Enriched event returned from API with LTV/COGS

## Usage

### Basic Usage

```bash
cd enrichment-test-suite
python3 enrich_csv_with_test.py
```

### Requirements

- Python 3.x
- `curl` command available
- Input file named `order-complete.csv` in parent directory
- Network access to dev API endpoint

### Configuration

Edit these variables in `enrich_csv_with_test.py`:

```python
DEV_ENDPOINT = "https://experimentation.dev.apigw.legalzoom.com/marketing-feed/enrich-ltv"
INPUT_CSV = "order-complete.csv"
PROGRESS_INTERVAL = 100  # Report every N events
```

## What the Script Does

### 1. LTV Removal
The script removes these fields from a **copy** of each event before sending:
- Order-level: `properties.ltv`, `properties.cogs`, `properties.ltv_net`
- Product-level: `products[].ltv`, `products[].cogs`

**Important:** The original event in `FULL_EVENT_PAYLOAD` is preserved unmodified for comparison.

### 2. API Request
Sends the cleaned event to the enrichment endpoint via POST request.

### 3. Validation
Checks that the enriched response contains:
- ✓ `properties.ltv` (numeric value)
- ✓ `properties.cogs` (numeric value)
- ✓ `properties.ltv_net` (numeric value)
- ✓ Each product has `ltv` and `cogs`

### 4. Test Result
- **PASS**: All validations succeeded
- **FAIL**: Missing fields, invalid values, or API errors

## Progress Reporting

The script reports progress every 100 events:

```
[  100] Processed: 99 passed, 0 failed (99.0% pass rate, 4.9 events/sec)
[  200] Processed: 199 passed, 0 failed (99.5% pass rate, 4.9 events/sec)
...
```

## Final Summary

After completion, you'll see:

```
================================================================================
ENRICHMENT COMPLETE
================================================================================
Total Events:     1000
✓ PASSED:         1000 (100.0%)
✗ FAILED:         0 (0.0%)

Time Elapsed:     202.1 seconds
Average Rate:     4.9 events/sec

Output saved to: order-complete-enriched-20251119_173455.csv
================================================================================
✓ All tests PASSED!
```

## Performance

- **Processing Rate:** ~5 events/second
- **1,000 events:** ~3.4 minutes
- **10,000 events:** ~33 minutes (estimated)

## Test Results (2025-11-19)

Ran against dev server with 1000 Order Completed events:

| Metric | Value |
|--------|-------|
| Total Events | 1000 |
| Passed | 1000 (100%) |
| Failed | 0 (0%) |
| Total LTV | $296,685.31 |
| Total COGS | $57,965.01 |
| Total Net LTV | $242,137.73 |
| Avg LTV per Order | $296.69 |
| Avg COGS per Order | $57.97 |
| Processing Time | 202 seconds |
| Rate | 4.9 events/sec |

## Troubleshooting

### Connection Errors
```
✗ Request failed: HTTP 000
```
- Check that dev server is accessible
- Verify endpoint URL is correct
- Check network connectivity

### Timeout Errors
```
✗ Request failed: Request timeout
```
- Increase timeout in script (default: 30 seconds)
- Server may be overloaded

### Missing Fields
```
⚠ Response missing fields: properties.ltv, properties.cogs
```
- Enrichment API may not be working correctly
- Check server logs for errors
- Verify DEV_DATA_SCIENCE.LTV tables have data

### Parse Errors
```
✗ Failed to parse JSON
```
- Input CSV may have malformed JSON
- Check FULL_EVENT_PAYLOAD column format

## Notes

- The script uses `.copy()` and `deepcopy()` to preserve original events
- CSV handles multi-line JSON properly
- Output is flushed every 100 events to prevent data loss on interruption
- Can be interrupted with Ctrl+C - partial results are saved

## Example Output Analysis

To analyze results:

```python
import csv
import json

with open('order-complete-enriched-TIMESTAMP.csv', 'r') as f:
    reader = csv.DictReader(f)

    passed = sum(1 for row in reader if row['TEST_RESULT'] == 'PASS')
    failed = sum(1 for row in reader if row['TEST_RESULT'] == 'FAIL')

    print(f"Passed: {passed}, Failed: {failed}")
```

## Related Files

- **Main API Code:** `src/main/kotlin/com/legalzoom/experimentationapi/EventEnrichmentServiceImpl.kt`
- **Test Code:** `src/test/kotlin/com/legalzoom/experimentationapi/EventEnrichmentServiceTest.kt`
- **Config:** `src/main/resources/application-local.properties`
