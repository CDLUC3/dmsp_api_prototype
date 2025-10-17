import json
import pathlib

import json_lines
import pymysql.cursors
from jsonlines import jsonlines


def insert_related_works_batch(cursor: pymysql.cursors.DictCursor, data: list):
    sql = "INSERT INTO stagingRelatedWorks (dmpDoi,workDoi,hash,sourceType,score,scoreMax,doiMatch,contentMatch,authorMatches,institutionMatches,funderMatches,awardMatches) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql, data)


def insert_work_versions_batch(cursor: pymysql.cursors.DictCursor, data: list):
    sql = "INSERT INTO stagingWorkVersions (doi,hash,workType,publicationDate,title,abstractText,authors,institutions,funders,awards,publicationVenue,sourceName,sourceUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql, data)


def create_work_versions(input_path: pathlib.Path, output_path: pathlib.Path):
    seen = set()
    with json_lines.open(input_path) as in_file:
        with jsonlines.open(output_path, mode='w') as out_file:
            for row in in_file:
                work = row["work"]
                doi = work["doi"]
                if doi not in seen:
                    out_file.write(to_work_version(work))
                    seen.add(doi)


def to_work_version(data: dict) -> list:
    return [
        data["doi"],
        data["hash"],
        data["workType"],
        data["publicationDate"],
        data["title"],
        data["abstractText"],
        serialise_json(data["authors"]),
        serialise_json(data["institutions"]),
        serialise_json(data["funders"]),
        serialise_json(data["awards"]),
        data["publicationVenue"],
        data["source"]["name"],
        data["source"]["url"],
    ]


def create_related_works(input_path: pathlib.Path, output_path: pathlib.Path):
    with json_lines.open(input_path) as in_file:
        with jsonlines.open(output_path, mode='w') as out_file:
            for row in in_file:
                out_file.write(to_related_work(row))


def to_related_work(data: dict) -> list:
    return [
        data["dmpDoi"],
        data["work"]["doi"],
        data["work"]["hash"],
        "SYSTEM_MATCHED",
        data["score"],
        data["scoreMax"],
        serialise_json(data["doiMatch"]),
        serialise_json(data["contentMatch"]),
        serialise_json(data["authorMatches"]),
        serialise_json(data["institutionMatches"]),
        serialise_json(data["funderMatches"]),
        serialise_json(data["awardMatches"]),
    ]


def serialise_json(data) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def insert_work_versions(input_path: pathlib.Path, conn, batch_size: int = 1000):
    batch = []
    with json_lines.open(input_path) as in_file:
        for row in in_file:
            row[1] = bytes.fromhex(row[1])
            batch.append(row)

            if len(batch) >= batch_size:
                insert_work_versions_batch(conn, batch)
                batch = []

    if len(batch):
        insert_work_versions_batch(conn, batch)


def insert_related_works(input_path: pathlib.Path, conn, batch_size: int = 1000):
    batch = []
    with json_lines.open(input_path) as in_file:
        for row in in_file:
            row[2] = bytes.fromhex(row[2])
            batch.append(row)

            if len(batch) >= batch_size:
                insert_related_works_batch(conn, batch)
                batch = []

    if len(batch):
        insert_related_works_batch(conn, batch)


def print_table(cursor: pymysql.cursors.DictCursor, table_name: str):
    print(f"Table: {table_name}")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    results = cursor.fetchall()
    for row in results:
        print(row)


def update_related_works(conn, input_path: pathlib.Path, batch_size: int = 1000):
    work_versions_path = input_path.parent / "work_versions.jsonl"
    related_works_path = input_path.parent / "related_works.jsonl"
    create_work_versions(input_path, work_versions_path)
    create_related_works(input_path, related_works_path)

    try:
        with conn.cursor() as cursor:
            # Create temp tables
            cursor.callproc("create_related_works_staging_tables")

            # Load work versions
            insert_work_versions(work_versions_path, cursor, batch_size=batch_size)
            print_table(cursor, "stagingWorkVersions")

            # Load related works
            insert_related_works(related_works_path, cursor, batch_size=batch_size)
            print_table(cursor, "stagingRelatedWorks")

            # Call stored procedure to update
            cursor.callproc("batch_update_related_works")
            print_table(cursor, "works")
            print_table(cursor, "workVersions")
            print_table(cursor, "relatedWorks")

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
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        database="dmsp",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    input_path = pathlib.Path()
    update_related_works(conn, input_path)
