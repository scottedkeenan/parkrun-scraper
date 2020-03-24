import csv
import string

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_race_results_table(cursor):
    create_table_sql = """
    DROP TABLE IF EXISTS race_results;
    
    CREATE TABLE race_results (
        id serial PRIMARY KEY,
        event_name varchar, 
        event_number integer,
        position integer,
        name varchar,
        name_detail varchar,
        gender varchar,
        gender_detail varchar,
        age_group varchar,
        age_group_detail varchar,
        club varchar,
        time varchar,
        time_detail varchar,
        UNIQUE(event_name, event_number, position)
    );
    """
    cursor.execute(create_table_sql)


def update_race_results_postgres(cursor):
    race_results_filenames = {}
    with open('./parkrun/personal_results.csv', 'r') as personal_results_file:
        personal_results_reader = csv.reader(personal_results_file)
        for row in personal_results_reader:
            event_name = row[0].lower().replace(' ', '_')
            event_number = row[4]
            race_result_filename = '{}_{}.csv'.format(event_name, event_number)
            personal_results_reader = csv.reader(personal_results_file)
            race_results_filenames[race_result_filename] = {'event_name': event_name, 'event_number': event_number}

    for race_result_filename in race_results_filenames:
        with open('./parkrun/race_results/{}'.format(race_result_filename), 'r') as race_result:
            race_result_reader = csv.reader(race_result)
            row_num = 0

            for row in race_result_reader:
                print("Processing row {} of file {}".format(row_num, race_result_filename))
                row_num += 1

                name = (row[1])

                insert_row_sql = """
                    INSERT INTO race_results (
                        event_name, 
                        event_number,
                        position,
                        name,
                        name_detail,
                        gender,
                        gender_detail,
                        age_group,
                        age_group_detail,
                        club,
                        time,
                        time_detail
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                insert_row_data = (
                    race_results_filenames[race_result_filename]['event_name'].replace('_', ' '),
                    race_results_filenames[race_result_filename]['event_number'],
                    row[0],
                    string.capwords(row[1], ' '),
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9]
                )

                cursor.execute(insert_row_sql, insert_row_data)

conn = psycopg2.connect(user="postgres", password="postgres", host="172.19.0.2", dbname="parkrundb")
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()