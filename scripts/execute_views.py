import snowflake.connector
import yaml
import os
import reprlib

account = os.getenv("SNOWFLAKE_ACCOUNT")

print("RAW ACCOUNT VALUE:", repr(account))


conn = snowflake.connector.connect(
    account="mx71933.me-central2.gcp",        # hardcoded
    user="VIEW_AUTOMATION_USER",              # hardcoded
    password="TempPassword@123",              # hardcoded
    role="VIEW_AUTOMATION_ROLE",               # hardcoded
    warehouse="TEST_WH",                      # hardcoded
    database="TEST_DB"                        # hardcoded
)

cursor = conn.cursor()

for file in os.listdir("view_requests"):
    if not file.endswith(".yaml"):
        continue

    with open(f"view_requests/{file}") as f:
        data = yaml.safe_load(f)

    # 1️⃣ Check if mapping already exists (FIXED)
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

    # 2️⃣ Insert + create view only if not exists
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

cursor.close()
conn.close()
