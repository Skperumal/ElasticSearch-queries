from es_utils import ES
from create_querys import create_meat_query
from index_meat_drug_section_percolator import main as index_meat_drug_section_to_percolator
from index_diag_percolator import main as index_diag_to_percolator
from enc_level_to_percolator import main as enc_level_to_percolator
from enc_date_to_percolator import main as enc_date_to_percolator
import json
import csv
import time
import socket
from procedure_into_db import Procedure_into_DB
from OTP_logs import Log


class PerculatorScripts:
    def __init__(self, data, run_logs):
        url = r'http://localhost:9200'
        self.ES_CONN = ES(url)
        self.data = data
        self.DOC_TYPE = 'charts'
        self.QUERIES_DOC_TYPE = 'queries'
        self.fields_meat_drug_section = ['doc']
        self.enc_level_fields = ['doc']
        self.fields_diag = ['review_systems', 'history', 'exam', 'assessment', 'plan', 'family_history', 'social_history',
                            'medications', 'allergies', 'surgical_history', 'complaint', 'history_present_illness', 'other',
                            'problem_list', 'rfv', 'labs', 'radiology', 'exclude', 'initial', 'allergies']
        self.run_logs = run_logs

    
    def index_enc_level(self):
        enc_level_to_percolator(self.data, self.ES_CONN, self.QUERIES_DOC_TYPE, self.DOC_TYPE, self.enc_level_fields, self.run_logs)

    

if __name__ == '__main__':

    data = json.load(open(r'c:\ES\OTPconfig.json'))
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    data['HOSTNAME'] = hostname
    data['vers'] = '2.3'
    run_logs = Log(data['Log_Root_Path'], data['cac_index_version'])
    Proc = None

    try:
        if data['RUN_FROM'].lower() == 'api':
            Proc = Procedure_into_DB(data, run_logs)
            Proc.updateCACServiceStatus('WaveCacIndex', 2)


        PS = PerculatorScripts(data, run_logs)
        PS.index_enc_level()
        if data['RUN_FROM'].lower() == 'api' and Proc is not None:
            Proc.updateCACServiceStatus('Index', 1)
        run_logs.insert_log("*** END ***")
    except Exception as e:
        run_logs.insert_log("Error == > {0}".format(e))
        if Proc is not None:
            Proc.updateCACServiceStatus('Index', 3)
        run_logs.insert_log("*** ERROR ***")

