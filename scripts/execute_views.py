import sys
import os
import yaml
import subprocess
import snowflake.connector

# ------------------------------------------------------------------
# Inputs: list of CHANGED yaml files from GitHub Actions
# ------------------------------------------------------------------
files_to_process = sys.argv[1:]

if not files_to_process:
    print("No changed YAML files to process.")
    sys.exit(0)

GIT_BEFORE = os.getenv("GIT_BEFORE")

# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------
def load_tables_from_content(content: str) -> set:
    """
    Extract table names from YAML content.
    """
    if not content:
        return set()

    data = yaml.safe_load(content) or {}
    tables = data.get("tables", {})

    if isinstance(tables, dict):
        return set(tables.keys())

    raise ValueError("Invalid YAML format: 'tables' must be a dictionary")


def get_old_file_content(file_path: str) -> str:
    """
    Get file content from previous commit.
    If file did not exist before, return empty string.
    """
    if not GIT_BEFORE or GIT_BEFORE == "0000000000000000000000000000000000000000":
        return ""

    try:
        return subprocess.check_output(
            ["git", "show", f"{GIT_BEFORE}:{file_path}"],
            stderr=subprocess.DEVNULL
        ).decode()
    except subprocess.CalledProcessError:
        return ""


def resolve_config(defaults: dict, overrides: dict) -> dict:
    """
    Merge defaults with per-table overrides.
    """
    return {
        "src_db": overrides.get("src_db", defaults["src_db"]),
        "src_sch": overrides.get("src_sch", defaults["src_sch"]),
        "tgt_db": overrides.get("tgt_db", defaults["tgt_db"]),
        "tgt_sch": overrides.get("tgt_sch", defaults["tgt_sch"]),
    }

# ------------------------------------------------------------------
# Snowflake connection
# ------------------------------------------------------------------
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)

cursor = conn.cursor()

# ------------------------------------------------------------------
# Main processing loop
# ------------------------------------------------------------------
for file in files_to_process:
    print(f"\nProcessing file: {file}")

    with open(file) as f:
        new_content = f.read()

    old_content = get_old_file_content(file)

    new_tables = load_tables_from_content(new_content)
    old_tables = load_tables_from_content(old_content)

    added_tables = new_tables - old_tables

    if not added_tables:
        print("  No newly added tables in this file.")
        continue

    data = yaml.safe_load(new_content)

    defaults = data["defaults"]
    tables_cfg = data["tables"]

    for table_name in added_tables:
        print(f"  → New table detected: {table_name}")

        table_overrides = tables_cfg.get(table_name, {})
        cfg = resolve_config(defaults, table_overrides)

        src_db = cfg["src_db"]
        src_sch = cfg["src_sch"]
        tgt_db = cfg["tgt_db"]
        tgt_sch = cfg["tgt_sch"]

        # Safety check using metadata table
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM TEST_DB.TEST_SCH.MAP_RAW
            WHERE SRC_DB = '{src_db}'
              AND SRC_SCH = '{src_sch}'
              AND SRC_TABLE = '{table_name}'
              AND TGT_DB = '{tgt_db}'
              AND TGT_SCH = '{tgt_sch}'
        """)

        exists = cursor.fetchone()[0]

        if exists == 0:
            print(f"    Creating secure view for {table_name}")

            cursor.execute(f"""
                INSERT INTO TEST_DB.TEST_SCH.MAP_RAW
                (SRC_DB, SRC_SCH, SRC_TABLE, TGT_DB, TGT_SCH)
                VALUES
                ('{src_db}', '{src_sch}', '{table_name}',
                 '{tgt_db}', '{tgt_sch}')
            """)

            cursor.execute(f"""
                CALL TEST_DB.TEST_SCH.CREATE_SECURE_VIEW_PROC(
                    '{src_db}',
                    '{src_sch}',
                    '{table_name}',
                    '{tgt_db}',
                    '{tgt_sch}'
                )
            """)
        else:
            print("    Mapping already exists. Skipping.")

# ------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------
cursor.close()
conn.close()

print("\nSnowflake view automation completed successfully.")
