# Tableau Sync log review (2026-02-02)

## What happened
- The GitHub Actions job successfully called the Tableau Sync endpoint and streamed NDJSON progress messages from the service.
- The service fetched **456,060** raw rows and grouped them into **15,497** unique records, then processed those records in 8 chunks (3 parallel).
- The run ended with **15,500 failed** and **-3 inserted**, indicating nearly all records failed during main processing, followed by no backfill updates.
- The service still cleared the raw staging table and reported campaign group stats.

## Primary error signal
- The final response includes repeated errors of **"stack depth limit exceeded"** at every 500-record interval, and the backfill phase also reported **15,497 errors**.

## Key implications
- The processing logic likely hit a recursive/stack depth guard (database or application stack), causing failures for essentially all chunks.
- Because the job still cleared the raw table, the raw input was removed even though processing failed.

## Suggested next checks
1. Inspect the service logs around the processing step that handles grouped records for recursive calls or deeply nested SQL (common sources of stack depth errors).
2. Verify whether the endpoint is using a database function or trigger that can exceed stack depth when handling large batches.
3. Consider reducing batch size or disabling recursion in any SQL/ORM operation to confirm the root cause.

## Evidence (excerpted from the run output)
- Fetched 456,060 raw rows and grouped into 15,497 unique records.
- Main processing completed with -3 processed and 15,500 failed.
- Final error list includes repeated "stack depth limit exceeded" entries and backfill errors for all records.
