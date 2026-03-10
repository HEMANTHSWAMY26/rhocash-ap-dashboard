# Directive: Scrape Jobs

**Goal:** Collect Accounts Payable job postings from US companies.

## Inputs
- Search Keywords:
  - Accounts Payable
  - Accounts Payables
  - Accounts Payable Clerk
  - Accounts Payable Specialist
  - Accounts Payable Analyst
  - Accounts Payable Manager
  - Full Cycle Accounts Payable
- Location Filter: United States only
- Sources: Indeed, LinkedIn, Monster, Aston Carter, JobRight, AccountingCrossing, Robert Half

## Tools/Scripts
- Execution Tool: `execution/run_apify_scraper.py`
- Required Environment Variables: `APIFY_API_TOKEN`

## Outputs
- File: `.tmp/raw_jobs.json`
- Format: JSON array of job listing objects.

## Edge Cases
- If the Apify actor fails due to rate limits or timeouts, the script should retry or gracefully exit with an error for the agent to catch.
- Ensure the output strictly contains US-based jobs.
