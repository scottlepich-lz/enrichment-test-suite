#!/usr/bin/env python3
"""
Enrich events from CSV with test validation.

Output columns:
1. EVENT_ID - ID from source CSV
2. TEST_RESULT - PASS/FAIL based on enrichment success
3. FULL_EVENT_PAYLOAD - Original event
4. ENRICHED_RESPONSE - Enriched event from API
"""

import csv
import json
import sys
import subprocess
import tempfile
import os
from datetime import datetime
import time

# Configuration
DEV_ENDPOINT = "https://experimentation.dev.apigw.legalzoom.com/marketing-feed/enrich-ltv"
INPUT_CSV = "order-complete.csv"
OUTPUT_CSV = f"order-complete-enriched-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
PROGRESS_INTERVAL = 100  # Report progress every N events

def remove_ltv_fields(event_dict):
    """Remove ltv, ltv_net, cogs from event properties and products"""
    if 'properties' in event_dict:
        props = event_dict['properties']

        # Remove order-level fields
        props.pop('ltv', None)
        props.pop('ltv_net', None)
        props.pop('cogs', None)

        # Remove product-level fields
        if 'products' in props and isinstance(props['products'], list):
            for product in props['products']:
                product.pop('ltv', None)
                product.pop('cogs', None)

    return event_dict

def send_event_to_enrichment(event_dict):
    """Send event to enrichment endpoint and return enriched response"""
    # Save to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
        json.dump(event_dict, tmp)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ['curl', '-s', '-w', '\n%{http_code}',
             '-X', 'POST',
             '-H', 'Content-Type: application/json',
             '-d', f'@{tmp_path}',
             DEV_ENDPOINT],
            capture_output=True,
            text=True,
            timeout=30
        )

        # Parse response
        lines = result.stdout.strip().split('\n')
        http_code = lines[-1] if lines else '000'
        body = '\n'.join(lines[:-1]) if len(lines) > 1 else ''

        # Clean up temp file
        os.unlink(tmp_path)

        if http_code != '200':
            return None, f"HTTP {http_code}"

        try:
            enriched = json.loads(body)
            return enriched, None
        except:
            return None, "Invalid JSON response"

    except subprocess.TimeoutExpired:
        os.unlink(tmp_path)
        return None, "Request timeout"
    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return None, str(e)

def validate_enrichment(enriched_response):
    """
    Validate that enrichment was successful.
    Returns: ("PASS" or "FAIL", reason)
    """
    if not enriched_response:
        return "FAIL", "No response"

    if 'error' in enriched_response:
        return "FAIL", enriched_response['error']

    props = enriched_response.get('properties', {})

    # Check required fields
    missing = []
    if 'ltv' not in props:
        missing.append('ltv')
    if 'cogs' not in props:
        missing.append('cogs')
    if 'ltv_net' not in props:
        missing.append('ltv_net')

    if missing:
        return "FAIL", f"Missing fields: {', '.join(missing)}"

    # Validate values are numbers
    try:
        ltv = float(props['ltv'])
        cogs = float(props['cogs'])
        ltv_net = float(props['ltv_net'])
    except (ValueError, TypeError) as e:
        return "FAIL", f"Invalid numeric values: {e}"

    # Check products have ltv and cogs
    products = props.get('products', [])
    for idx, product in enumerate(products):
        if 'ltv' not in product:
            return "FAIL", f"Product {idx} missing ltv"
        if 'cogs' not in product:
            return "FAIL", f"Product {idx} missing cogs"

    return "PASS", "All validations passed"

def main():
    print(f"Enrichment CSV Processor with Validation")
    print(f"Input:  {INPUT_CSV}")
    print(f"Output: {OUTPUT_CSV}")
    print(f"Endpoint: {DEV_ENDPOINT}")
    print("=" * 80)

    stats = {
        'total': 0,
        'passed': 0,
        'failed': 0
    }

    start_time = time.time()

    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as infile, \
             open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.DictReader(infile)

            # Create writer with new column order
            fieldnames = ['EVENT_ID', 'TEST_RESULT', 'FULL_EVENT_PAYLOAD', 'ENRICHED_RESPONSE']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for idx, row in enumerate(reader, start=1):
                stats['total'] += 1

                # Progress reporting
                if idx % PROGRESS_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    pass_rate = (stats['passed'] / idx * 100) if idx > 0 else 0
                    print(f"[{idx:5d}] Processed: {stats['passed']} passed, {stats['failed']} failed "
                          f"({pass_rate:.1f}% pass rate, {rate:.1f} events/sec)")

                event_id = row.get('EVENT_ID', f'unknown_{idx}')

                # Parse original event
                try:
                    original_event = json.loads(row['FULL_EVENT_PAYLOAD'])
                except Exception as e:
                    result = "FAIL"
                    enriched_json = json.dumps({"error": f"Parse error: {str(e)}"})
                    stats['failed'] += 1

                    writer.writerow({
                        'EVENT_ID': event_id,
                        'TEST_RESULT': result,
                        'FULL_EVENT_PAYLOAD': row['FULL_EVENT_PAYLOAD'],
                        'ENRICHED_RESPONSE': enriched_json
                    })
                    continue

                # Remove LTV fields from source event
                cleaned_event = remove_ltv_fields(original_event.copy())

                # Send to enrichment endpoint
                enriched, error = send_event_to_enrichment(cleaned_event)

                if error:
                    result = "FAIL"
                    enriched_json = json.dumps({"error": error})
                    stats['failed'] += 1
                else:
                    # Validate enrichment
                    test_result, reason = validate_enrichment(enriched)
                    result = test_result

                    if test_result == "PASS":
                        stats['passed'] += 1
                        enriched_json = json.dumps(enriched)
                    else:
                        stats['failed'] += 1
                        enriched_json = json.dumps({
                            "error": reason,
                            "response": enriched
                        })

                # Write output row
                writer.writerow({
                    'EVENT_ID': event_id,
                    'TEST_RESULT': result,
                    'FULL_EVENT_PAYLOAD': row['FULL_EVENT_PAYLOAD'],
                    'ENRICHED_RESPONSE': enriched_json
                })

                # Flush periodically
                if idx % 100 == 0:
                    outfile.flush()

        # Print final summary
        elapsed = time.time() - start_time
        rate = stats['total'] / elapsed if elapsed > 0 else 0

        print("\n" + "=" * 80)
        print("ENRICHMENT COMPLETE")
        print("=" * 80)
        print(f"Total Events:     {stats['total']}")
        print(f"✓ PASSED:         {stats['passed']} ({stats['passed']/stats['total']*100:.1f}%)")
        print(f"✗ FAILED:         {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
        print(f"\nTime Elapsed:     {elapsed:.1f} seconds")
        print(f"Average Rate:     {rate:.1f} events/sec")
        print(f"\nOutput saved to: {OUTPUT_CSV}")
        print("=" * 80)

        if stats['passed'] == stats['total']:
            print("✓ All tests PASSED!")
            return 0
        elif stats['passed'] > stats['total'] * 0.95:
            print("⚠ >95% tests passed")
            return 0
        else:
            print(f"⚠ {stats['failed']} tests failed")
            return 1

    except FileNotFoundError:
        print(f"\n✗ Error: Input CSV not found: {INPUT_CSV}")
        return 1
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\n\n⚠ Interrupted by user after {elapsed:.1f} seconds")
        print(f"Processed {stats['total']} events ({stats['passed']} passed)")
        print(f"Partial output saved to: {OUTPUT_CSV}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
