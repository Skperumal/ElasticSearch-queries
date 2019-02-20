import elasticsearch as es
import elasticsearch_dsl as edsl
import re
import certifi
from elasticsearch_dsl import analyzer, tokenizer

class ES:
    def __init__(self, url):
        if 'localhost' in url:
            self.conn = edsl.connections.connections.create_connection(hosts=[url])
        else:
            auth = re.search('https\:\/\/(.*)\@', url).group(1).split(':')
            host = url.replace('https://%s:%s@' % (auth[0], auth[1]), '')

            # Connect to cluster over SSL using auth for best security:
            es_header = [{
                'host': host,
                'port': 443,
                'use_ssl': True,
                'http_auth': (auth[0], auth[1]),
                'verify_certs': True,
                'ca_certs': certifi.where(),

            }]
            self.conn = edsl.connections.connections.create_connection(hosts=['localhost'], timeout=1000)


    def create_index(self, index_name, run_logs):
        """ Creates a new index, destroying any existing index with the same
            name. """
        index = edsl.Index(index_name)

        index.settings(number_of_shards=3)
        if index.exists():
            index.delete()
            run_logs.insert_log('old {0} index deleted'.format(index_name))

        if index_name == "enc_dates_NOT_NOW":
            run_logs.insert_log("entered analyzer for {0}".format(index_name))
            enc_analyzer = analyzer('enc_analyzer', tokenizer="whitespace",
                filter=['lowercase'])
            index.analyzer(enc_analyzer)

        index.create()
        run_logs.insert_log('new {0} index created'.format(index_name))
        return index

    def add_mappings(self, index_name, fields, queries_doctype, chart_doctype):
        """ Add document mappings to the index.  This creates two mappings -
        one for the queries we'll be indexing to percolate against, and a second
        for preprocessing the chart documents we want to percolate.
        This would be where any special options should be applied to use different
        highlighters, special analyzers, etc. """
        # Add two mappings to the index.  One for the queries we'll index,
        # and one for the documents that will be percolated.
        # query_mapping = edsl.Mapping(queries_doctype)
        # query_mapping.field('query', 'percolator')
        # query_mapping.field('type', type='keyword')
        # query_mapping.field('code', type='keyword')
        # query_mapping.save(index_name)

        # NOTE: We have to use this lower-level method due to the fact that the
        # elasticsearch_dsl library doesn't yet support percolator Field type


        percolator_mapping = {

            'properties': {
                'query': {
                    'type': 'percolator'

                }
            }
        }

        self.conn.indices.put_mapping(
            doc_type=queries_doctype,
            body=percolator_mapping,
            index=index_name,
        )
        chart_mapping = edsl.Mapping(chart_doctype)

        for field in fields:
            if index_name == "enc_dates_NOT_NOW":
                chart_mapping.field(field, {"type":'text', 'analyzer':'enc_analyzer'})
            else:
                chart_mapping.field(field, {"type": 'text'})
        chart_mapping.save(index_name)

    def index_queries(self, index_name, queries_doctype, queries, run_logs):
        bulk_metadata = {
            '_index': index_name,
            '_type': queries_doctype,
        }
        for query in queries:
            query.update(bulk_metadata)
        count, errors = es.helpers.bulk(self.conn, queries)
        run_logs.insert_log(errors)
        return count, errors

