import csv
import string
import os
from os import listdir
from os.path import isfile, join

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#https://info.crunchydata.com/blog/easy-postgresql-10-and-pgadmin-4-setup-with-docker

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


def update_personal_race_results_postgres(cursor):
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


def update_event_race_results_postgres(cursor, event_name=''):
    results_path = './parkrun/race_results'
    if os.path.exists('./parkrun/race_results/'):
        race_results_filenames = [f for f in listdir(results_path) if (isfile(join(results_path, f)) and event_name in f and f[-4:] == '.csv')]
        print(race_results_filenames)
    else:
        return

    for race_result_filename in race_results_filenames:
        with open('./parkrun/race_results/{}'.format(race_result_filename), 'r') as race_result:
            race_result_reader = csv.reader(race_result)
            row_num = 0

            for row in race_result_reader:
                print("Processing row {} of file {}".format(row_num, race_result_filename))
                row_num += 1
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
                    race_result_filename.split('_')[0],
                    race_result_filename[:-4].split('_')[1],
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


def get_overall_points_competition(cursor, event_name=''):

    get_sql = """
        SELECT 	name, SUM(
        CASE WHEN position <= 100 THEN (100-(position-1))
        ELSE 0 END)
        AS points
        FROM race_results
        WHERE event_name = 'doddingtonhall'
        AND name != 'Unknown'
        GROUP BY name
        ORDER BY points desc
        """

    result = cursor.execute(get_sql)
    return cursor.fetchall()

conn = psycopg2.connect(user="postgres", password="postgres", host="172.19.0.3", dbname="parkrundb")
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()


# create_race_results_table(cursor)
# update_personal_race_results_postgres(cursor)
# update_event_race_results_postgres(cursor, event_name='doddingtonhall')

overall_points = get_overall_points_competition(cursor)

for pos, runner in enumerate(overall_points):
    if pos > 99:
        break
    print(pos+1, '|', runner[0], '|', runner[1])
