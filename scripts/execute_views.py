import snowflake.connector
import yaml
import os

def env(name):
    value = os.getenv(name)
    if not value:
        raise Exception(f"Missing environment variable: {name}")
    return value.strip().replace("\n", "").replace("\r", "")

conn = snowflake.connector.connect(
    account=env("SNOWFLAKE_ACCOUNT"),
    user=env("SNOWFLAKE_USER"),
    password=env("SNOWFLAKE_PASSWORD"),
    role=env("SNOWFLAKE_ROLE"),
    warehouse=env("SNOWFLAKE_WAREHOUSE"),
    database=env("SNOWFLAKE_DATABASE")
)

cursor = conn.cursor()

for file in os.listdir("view_requests"):
    if not file.endswith(".yaml"):
        continue

    with open(f"view_requests/{file}") as f:
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

cursor.close()
conn.close()
