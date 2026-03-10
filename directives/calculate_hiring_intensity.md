# Directive: Calculate Hiring Intensity

**Goal:** Assign a hiring intensity score to each company based on the volume of their active job listings.

## Inputs
- Input File: `.tmp/processed_jobs.csv`
- Requirement: Group by 'Company Name'

## Logic
Assign an intensity tag using the following brackets based on job count:
- 1 job → Low
- 2-4 jobs → Medium
- 5+ jobs → High

## Tools/Scripts
- Execution Tool: `execution/calculate_hiring_intensity.py`

## Outputs
- File: `.tmp/jobs_with_intensity.csv`
- Format: Contains the base processed dataset with an appended `Hiring Intensity` column.

## Edge Cases
- Ensure exact spelling/capitalization matches for the company names before grouping.
