# Directive: Update Google Sheet

**Goal:** Append the processed job leads with their hiring intensity scores to the central Google Sheet database.

## Inputs
- Input File: `.tmp/jobs_with_intensity.csv`
- Environment Variables:
  - `GOOGLE_SHEET_ID`
  - `GOOGLE_SERVICE_ACCOUNT_JSON`

## Target Sheet Structure
The spreadsheet should contain:
1. **Master Sheet:** All leads aggregated on an ongoing basis.
2. **Daily Sheet:** A new sheet created/updated daily formatted as `DailyJobs-YYYY-MM-DD`.

### Expected Columns
- Company
- Job Title
- Location (Combined City + State)
- Posted Date
- Source
- Job URL
- Hiring Intensity
- Timestamp Added (Current system time of execution)

## Tools/Scripts
- Execution Tool: `execution/update_google_sheet.py`

## Outputs
- A confirmed update append to the designated Google Sheet ID.

## Edge Cases
- Handle API quota limits using exponential backoff.
- If the Daily Sheet doesn't exist, the script must create it programmatically before appending.
