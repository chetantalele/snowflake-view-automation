# snowflake-view-automation

# Snowflake Secure View Automation

## Flow
1. User adds YAML in `view_requests/`
2. Raises PR to `main`
3. Merge triggers GitHub Actions
4. Automation:
   - Checks MAP_RAW
   - Inserts metadata if missing
   - Creates secure view

## Auth
Snowflake username/password via GitHub Secrets
