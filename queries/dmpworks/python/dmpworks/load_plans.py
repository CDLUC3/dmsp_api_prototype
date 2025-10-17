import pathlib

import json_lines
import pymysql
import pymysql.cursors


def insert_plans(cursor: pymysql.cursors.DictCursor, data: list):
    batch = []
    for row in data:
        batch.append(row)

        if len(batch) > 100:
            sql = "INSERT INTO plans (id,projectId,versionedTemplateId,dmpId,visibility,status,languageId,featured,createdById,modifiedById) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            result = cursor.executemany(sql, batch)
            batch = []

    if len(batch):
        sql = "INSERT INTO plans (id,projectId,versionedTemplateId,dmpId,visibility,status,languageId,featured,createdById,modifiedById) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql, batch)


def get_dmp_dois(input_path: pathlib.Path):
    seen = set()
    results = []
    with json_lines.open(input_path) as in_file:
        for row in in_file:
            dmp_doi = row["dmpDoi"]
            if dmp_doi not in seen:
                results.append(dmp_doi)
                seen.add(dmp_doi)

    return results


def make_plans(dmp_dois: list[str]):
    return [
        [idx + 6, 1, 1, f"https://doi.org/{doi}", "PUBLIC", "COMPLETE", "en-US", 0, 1, 1]
        for idx, doi in enumerate(dmp_dois)
    ]


def main():
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        database="dmsp",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    input_path = pathlib.Path()
    dmp_dois = get_dmp_dois(input_path)
    data = make_plans(dmp_dois)

    try:
        with conn.cursor() as cursor:
            insert_plans(cursor, data)
        conn.commit()
    except pymysql.MySQLError as e:
        print(f"Database error occurred: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
