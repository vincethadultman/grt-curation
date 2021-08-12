# grt-curation-metrics
Python jobs to pull data from The Graph and publish actionable metrics for the discerning GRT curator.

## Requirements
- Etherscan API key added to query.py to pull all blocks for the queries
- dotmap 
- gql
- gspread
- Google's python API libraries (to publish CSVs to Drive and spreadsheets)

Google Drive credential and token jsons need to be placed into ./gspread as well as .config/gspread for the upload/publish

