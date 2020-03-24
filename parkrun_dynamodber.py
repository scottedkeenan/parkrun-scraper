import csv
import boto3


def update_personal_results_database(client, table_name='personal-results'):
    with open('./parkrun/personal_results.csv', 'r') as personal_results_file:
        personal_results_reader = csv.reader(personal_results_file)
        for row in personal_results_reader:
            response = client.put_item(
                TableName = table_name,
                Item = {
                    'date' : {
                        'S': row[2]
                    },
                    'event-name': {
                        'S': row[0]
                    },
                    'event_url': {
                        'S': row[1]
                    },
                    'run_date_url': {
                        'S': row[3]
                    },
                    'run_number': {
                        'N': row[4]
                    },
                    'run_number_url': {
                        'S': row[5]
                    },
                    'pos': {
                        'N': row[6]
                    },
                    'time': {
                        'S': row[7]
                    },
                    'age_grade': {
                        'N': row[8].strip('%')
                    },
                    'pb': {
                        'BOOL': True if row[9].strip() == 'PB' else False
                    }

                }

            )
            print(response)


def update_race_results_database(client, table_name='race-results'):
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

            item_batch = {table_name: []}

            for row in race_result_reader:
                print("Processing row {} of file {}".format(row_num, race_result_filename))
                row_num += 1

                item_mapping = {
                    'event_name_number': {'type': 'S', 'data': '{}_{}'.format(
                        race_results_filenames[race_result_filename]['event_name'],
                        race_results_filenames[race_result_filename]['event_number'])},
                    'position': {'type': 'N', 'data': row[0]},
                    'name': {'type': 'S', 'data': row[1]},
                    'name_detail': {'type': 'S', 'data': row[2]},
                    'gender': {'type': 'S', 'data': row[3]},
                    'gender_detail': {'type': 'S', 'data': row[4]},
                    'age_group': {'type': 'S', 'data': row[5]},
                    'age_group_detail': {'type': 'S', 'data': row[6]},
                    'club': {'type': 'S', 'data': row[7]},
                    'time': {'type': 'S', 'data': row[8]},
                    'time_detail': {'type': 'S', 'data': row[9]},
                }

                item = {}

                for mapping in item_mapping:
                    if item_mapping[mapping]['data'] != '':
                        item[mapping] = {item_mapping[mapping]['type']: item_mapping[mapping]['data']}

                item_batch[table_name].append({'PutRequest': {'Item': item}})

                if len(item_batch[table_name]) == 25:
                    response = client.batch_write_item(RequestItems=item_batch)
                    if response['UnprocessedItems']:
                        item_batch = response['']
                    else:
                        item_batch = {table_name: []}
                    print(response)


session = boto3.Session(profile_name='personal')
dynamodb_client = session.client('dynamodb')

update_personal_results_database(dynamodb_client)
update_race_results_database(dynamodb_client, 'batch-test-table')