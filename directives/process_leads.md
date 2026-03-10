# Directive: Process Leads

**Goal:** Process and clean raw job data into structured leads.

## Inputs
- Input File: `.tmp/raw_jobs.json`

## Tasks
1. Parse raw JSON jobs.
2. Remove duplicate job postings based on Job URL or Company + Title + City combo.
3. Normalize company names (e.g., "Company Inc." -> "Company").
4. Extract required fields.

## Required Fields
Ensure the final dataset contains exactly these columns:
- Company Name
- Job Title
- Job Description
- City
- State
- Country
- Posted Date
- Job URL
- Source

## Tools/Scripts
- Execution Tool: `execution/process_jobs.py`

## Outputs
- File: `.tmp/processed_jobs.csv`
- Format: CSV file containing the deduplicated, structured leads.

## Edge Cases
- Missing locations: Fill with "Unknown" or parse from Job Description if possible.
- Invalid dates: Normalize all posted dates to `YYYY-MM-DD` string format.
