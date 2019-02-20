import csv
from decrypt import DECRYPT_MODULE

def decrypt_list(data):
    return [DECRYPT_MODULE(i) for i in data]


def get_csv_data(data, file_name):
    csv_file_path = r'{0}\{1}\{2}'.format(data['STATICFILES_PATH'], 'ES_QUERIES', file_name)
    with open(csv_file_path, 'r') as csv_file:
        data = csv.reader(csv_file)
        next(data)
        for row in data:
            yield decrypt_list(row)


def push_queries_to_percolator(ES_CONN, index_name, queries_doctype, queries, run_logs):
    try:
        ct, errors = ES_CONN.index_queries(index_name, queries_doctype, queries, run_logs)
        return True
    except Exception as e:
        return None
