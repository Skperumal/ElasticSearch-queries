from utils import get_csv_data
import time
from encounter_level_querys import create_start_word_querys,create_end_word_querys,create_sign_word_querys,\
    create_prefix_word_querys,create_provider_word_querys,create_codable_word_querys,create_pos_word_querys,create_facetoface_word_querys,\
    create_credntial_word_querys,create_doctor_name_word_querys


def push_queries_to_percolator(ES_CONN, index_name, queries_doctype, queries, run_logs):
    try:
        ct, errors = ES_CONN.index_queries(index_name, queries_doctype, queries, run_logs)
        return True
    except Exception as e:
        return None


def index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, file_name, create_query_func, run_logs):
    queries = []
    count = 0
    run_logs.insert_log('going to start indexing {0} queries'.format(index_name))
    if file_name:
        for row in get_csv_data(data, file_name):
            queries.append(create_query_func(row, fields_enc_level))
            if len(queries) == 1000:
                response = push_queries_to_percolator(ES_CONN, index_name, QUERIES_DOC_TYPE, queries, run_logs)
                while not response:
                    run_logs.insert_log('indexing error.. going to index again in 5 sec from {0}'.format(file_name))
                    time.sleep(5)
                    response = push_queries_to_percolator(ES_CONN, index_name, QUERIES_DOC_TYPE, queries, run_logs)
                count += 1000
                run_logs.insert_log('sucessfully indexed {0} from {1} queries ...'.format(count, file_name))
                queries = []
    # else:
    #     file_name="DATE"
    #     queries=create_date_querys()

    if len(queries) > 0:
        response = push_queries_to_percolator(ES_CONN, index_name, QUERIES_DOC_TYPE, queries, run_logs)
        while not response:
            run_logs.insert_log('indexing error.. going to index again in 5 from {0}'.format(file_name))
            time.sleep(5)
            response = push_queries_to_percolator(ES_CONN, index_name, QUERIES_DOC_TYPE, queries, run_logs)

    run_logs.insert_log('Sucessfully indexed {0} queries from {1}'.format(count + len(queries), file_name))


def main(data, ES_CONN, QUERIES_DOC_TYPE, DOC_TYPE, fields_enc_level, run_logs):
    index_name = data['ENC_LEVEL_INDEX_NAME']
    ES_CONN.create_index(index_name, run_logs)
    ES_CONN.add_mappings(index_name, fields_enc_level, QUERIES_DOC_TYPE, DOC_TYPE)
    run_logs.insert_log("added mappings to {0} index".format(index_name))
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'START_WORDS.csv',
                            create_start_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'END_WORDS.csv',
                            create_end_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'SIGN_WORDS.csv',
                            create_sign_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'PROVIDER_WORDS.csv',
                            create_provider_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'POS_WORDS.csv',
                            create_pos_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'CODABLE_AND_NONCODABLE_WORDS.csv',
                            create_codable_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'PREFIX_WORDS.csv',
                            create_prefix_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'F2F.csv',
                            create_facetoface_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'DOCTOR_NAMES.csv',
                            create_doctor_name_word_querys,run_logs)
    index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, 'PROVIDER_CREDENTIALS.csv',
                            create_credntial_word_querys,run_logs)
    # index_queries_by_levels(data, ES_CONN, index_name, QUERIES_DOC_TYPE, fields_enc_level, None,create_date_querys,run_logs)
