import argparse
import json

import dotenv

from database import Database
from load import ExportFile
import os
from dotenv import load_dotenv
load_dotenv()

def main():
    parser = argparse.ArgumentParser('Command-line for Python task_1')
    parser.add_argument('students', help='Path to students file', default='/file_storage')
    parser.add_argument('rooms', help='Path to rooms file', default='/file_storage')
    parser.add_argument('format', choices=['xml', 'json'], help='choose format file')
    args = parser.parse_args()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", default=5433)
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    database_config = {
            'host': db_host,
            'port': db_port,
            'database': db_name,
            'user': db_user,
            'password': db_password
    }
    print(database_config)
    with open('rooms.json','r') as room_file:
        rooms = json.load(room_file)
    with open('students.json', 'r') as student_file:
        students = json.load(student_file)

    db = Database(database_config)
    db.insert_into_db_rooms(rooms)
    db.insert_into_db_students(students)
    final_result = {
    'rooms_and_count': db.number_and_students(),
    'small_avg_age': db.smallest_avg_age(),
    'largest_difference': db.largest_difference(),
    'different_sex': db.different_sex_room()
    }
    file_name = f'file_storage/result.{args.format}'
    export_file = ExportFile.export_file(args.format)
    export_file.export(final_result, file_name)

if __name__ == "__main__":
    main()