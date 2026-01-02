import sys
import snowflake.connector
import yaml
import os

files_to_process = sys.argv[1:]

if not files_to_process:
    print("No new YAML files to process.")
    exit(0)

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)

cursor = conn.cursor()

for file in files_to_process:
    print(f"Processing new file: {file}")

    with open(file) as f:
        data = yaml.safe_load(f)

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM TEST_DB.TEST_SCH.MAP_RAW
        WHERE SRC_DB = '{data['src_db']}'
          AND SRC_SCH = '{data['src_sch']}'
          AND SRC_TABLE = '{data['src_table']}'
          AND TGT_DB = '{data['tgt_db']}'
          AND TGT_SCH = '{data['tgt_sch']}'
    """)

    exists = cursor.fetchone()[0]

    if exists == 0:
        cursor.execute(f"""
            INSERT INTO TEST_DB.TEST_SCH.MAP_RAW
            (SRC_DB, SRC_SCH, SRC_TABLE, TGT_DB, TGT_SCH)
            VALUES
            ('{data['src_db']}', '{data['src_sch']}',
             '{data['src_table']}', '{data['tgt_db']}',
             '{data['tgt_sch']}')
        """)

        cursor.execute(f"""
            CALL TEST_DB.TEST_SCH.CREATE_SECURE_VIEW_PROC(
                '{data['src_db']}',
                '{data['src_sch']}',
                '{data['src_table']}',
                '{data['tgt_db']}',
                '{data['tgt_sch']}'
            )
        """)
    else:
        print("Mapping already exists. Skipping.")

cursor.close()
conn.close()
