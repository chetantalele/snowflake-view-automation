import sys
import os
import yaml
import subprocess
import snowflake.connector

files_to_process = sys.argv[1:]
if not files_to_process:
    print("No changed YAML files to process.")
    sys.exit(0)

GIT_BEFORE = os.getenv("GIT_BEFORE")

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def load_tables(content):
    if not content:
        return set()
    data = yaml.safe_load(content) or {}
    return set(data.get("tables", {}).keys())

def get_old_content(file):
    if not GIT_BEFORE or GIT_BEFORE.startswith("000000"):
        return ""
    try:
        return subprocess.check_output(
            ["git", "show", f"{GIT_BEFORE}:{file}"],
            stderr=subprocess.DEVNULL
        ).decode()
    except subprocess.CalledProcessError:
        return ""

def resolve(defaults, overrides):
    return {
        "src_db": overrides.get("src_db", defaults["src_db"]),
        "src_sch": overrides.get("src_sch", defaults["src_sch"]),
        "tgt_db": overrides.get("tgt_db", defaults["tgt_db"]),
        "tgt_sch": overrides.get("tgt_sch", defaults["tgt_sch"]),
    }

# ----------------------------------------------------
# Connection 1 → Metadata INSERT / DELETE
# ----------------------------------------------------
meta_conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)
meta_cursor = meta_conn.cursor()

# ----------------------------------------------------
# Connection 2 → Stored Procedure execution
# ----------------------------------------------------
proc_conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)
proc_cursor = proc_conn.cursor()

# ----------------------------------------------------
# Main logic
# ----------------------------------------------------
for file in files_to_process:
    print(f"\nProcessing file: {file}")

    new_content = open(file).read()
    old_content = get_old_content(file)

    new_tables = load_tables(new_content)
    old_tables = load_tables(old_content)

    added_tables = new_tables - old_tables
    if not added_tables:
        print("  No new tables.")
        continue

    data = yaml.safe_load(new_content)
    defaults = data["defaults"]
    tables = data["tables"]

    for table in added_tables:
        cfg = resolve(defaults, tables.get(table, {}))

        src_db = cfg["src_db"]
        src_sch = cfg["src_sch"]
        tgt_db = cfg["tgt_db"]
        tgt_sch = cfg["tgt_sch"]

        try:
            print(f"  Inserting metadata for {table}")

            meta_cursor.execute(f"""
                INSERT INTO TEST_DB.TEST_SCH.MAP_RAW
                (SRC_DB, SRC_SCH, SRC_TABLE, TGT_DB, TGT_SCH)
                VALUES
                ('{src_db}', '{src_sch}', '{table}',
                 '{tgt_db}', '{tgt_sch}')
            """)

            print("  Calling procedure")

            proc_cursor.execute(f"""
                CALL TEST_DB.TEST_SCH.CREATE_SECURE_VIEW_PROC(
                    '{src_db}',
                    '{src_sch}',
                    '{table}',
                    '{tgt_db}',
                    '{tgt_sch}'
                )
            """)

            print("   View created successfully")

        except Exception as e:
            print(f"   View creation failed: {e}")
            print("  Deleting metadata")

            meta_cursor.execute(f"""
                DELETE FROM TEST_DB.TEST_SCH.MAP_RAW
                WHERE SRC_DB = '{src_db}'
                  AND SRC_SCH = '{src_sch}'
                  AND SRC_TABLE = '{table}'
                  AND TGT_DB = '{tgt_db}'
                  AND TGT_SCH = '{tgt_sch}'
            """)

            raise

# ----------------------------------------------------
# Cleanup
# ----------------------------------------------------
meta_cursor.close()
meta_conn.close()
proc_cursor.close()
proc_conn.close()

print("\nSnowflake view automation completed.")
