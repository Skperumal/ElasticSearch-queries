



def fields_update(field):
    res = {field: {
        "fragment_size": 50,
        "number_of_fragments": 3,
        "type": "plain",
        "fragmenter":"span"
    }}
    return res


def create_start_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0],
                    "boost": row[1],
                }
            }
            }
        }
    },
        "q_level": 'START_WORD',
        "codeable_check": row[2],
        "pair_start_words": row[3],
        "SWNL_word_count": row[4],
        "SW_IsActive": row[5],
        "start_word": row[0],
        "highlight": {
            "fields": fields
        }
    }
    return query


def create_facetoface_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
                 "multi_match": {
                     "fields": "doc",
                     "query": row[0],
                     "slop": int(row[1]),
                     "type": "phrase"
                 }
             },
        "q_level" : 'F2F_WORD',
            "facetofaceword": row[0],
        "highlight": {
            "fields": fields
        }
    }
    return query

def create_doctor_name_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
                 "multi_match": {
                     "fields": "doc",
                     "query": row[0],
                     "type": "phrase"
                 }
             },
        "q_level" : 'DOCTOR_NAMES',
        "DOCTORNAME": row[0],
        "NAME_TYPE": row[1],
        "highlight": {
            "fields": fields
        }
    }
    return query

def create_credntial_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
                 "multi_match": {
                     "fields": "doc",
                     "query": row[0],
                     "type": "phrase"
                 }
             },
        "q_level" : 'CRDENTIALS',
        "CREDENTIALS": row[0],
        "CREDEN_TYPE": row[1],
        "priority": row[2],
        "case": row[3],
        "highlight": {
            "fields": fields
        }
    }
    return query

def create_end_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0],
                    "boost": row[1],
                }
            }
            }
        }
    },
        "q_level" : 'END_WORD',
        "end_word": row[0],
        "end_pair_word": row[2],
        "ew_behaviour": row[3],
        "is_active": row[4],
        "highlight": {
            "fields": fields
        }
    }
    return query


def create_sign_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0],
                    "boost": row[1],
                }
            }
            }
        }
    },
        "q_level" : 'SIGN_WORD',
        "sign_word": row[0],
        "line_count": row[2],
        "highlight": {
            "fields": fields
        }
    }
    return query

def create_codable_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0]
                    # "slop": int(row[3])
                }
            }
            }
        }
    },
        "q_level" : 'CODABLE_WORD',
        "codable_word": row[0],
        "type_word": row[1],
        "nc_sw": row[2],
        "slop": int(row[3]),
        "nc_ln_split": int(row[4]),
        "highlight": {
            "fields": fields
        }
    }
    return query


def create_provider_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {
        "query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0]
                }
            }
            }
        }
    },
        "q_level" : 'PROVIDER_WORD',
        "provider_word": row[0],
        "value": row[1],
        "flag": row[2],
        "highlight": {
            "fields": fields
        }
    }
    return query


def create_prefix_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[0]
                }
            }
            }
        }
    },
        "q_level": 'PREFIX_WORD',
        "prefix_word": row[0],
        "type": row[1],
        "flag": row[2],
        "start_pair": row[3],
        "highlight": {
            "fields": fields
        }
    }
    return query


def create_pos_word_querys(row, fields_enc_level):
    fields = {}
    fields_map = [(i) for i in fields_enc_level]
    field_total = []

    for i in fields_map:
        fields.update(fields_update(i))
        field_total.append(i)
    query = {"query": {
        "bool": {
            "should": {"match_phrase": {
                "doc": {
                    "query": row[1]
                }
            }
            }
        }
    },
        "q_level" : 'POS_WORD',
        "type": row[0],
        "keyword": row[1],
        "value": row[2],
        "highlight": {
            "fields": fields
        }
    }
    return query
from datetime import datetime, timedelta
def create_date_querys():
    no_of_days = 3000
    base = datetime.today()
    date_list = [base - timedelta(days=x) for x in range(0, no_of_days)]
    list_of_queries = []
    for each_date_obj in date_list:
        # print(each_date_obj,"ddd")
        search_query = {
            "query": {
                "bool": {

                    "should": [
                        # Adding for Atena
                        {"match_phrase": {"doc": each_date_obj.strftime("%Y-%m-%d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%#m-%#d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%#m-%d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%m-%#d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1%d/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1%d/%Y")}},

                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%m-%#d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%m-%d")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y-%m-%d")}},

                        # Below 2 are recent adds
                        {"match_phrase": {"doc": each_date_obj.strftime("%Y-%B-%#d")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%Y-%B-%d")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%m-%d-%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m-%d-%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m-%d-%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m-%d-%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m-%#d-%y")}},

                        # '''RECENTLY EXCLUDED'''

                        # {"match_phrase": {"doc": each_date_obj.strftime("%#d-%#m-%y")}},
                        # '''RECENTLY EXCLUDED'''

                        # {"match_phrase": {"doc": each_date_obj.strftime("%Y/%m/%d")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m/%d/%Y")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%d-%b-%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%d-%B-%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%b-%d-%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%B-%d-%Y")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%m %d %Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m %#d %Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m/%d/%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m/%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m/%#d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m/%#d/%Y")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%b %d %Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%B %d %Y")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%b %e %Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%B %e %Y")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%b %d,%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%B %d,%Y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%b %d,%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%B %e,%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%d1%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%#d1%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m1%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("1%#m/%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("1%#m1%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("1%#m/%d1%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m %d %y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#m %d %y")}},

                        # FOR those date with trailing of begining underscore
                        {"match_phrase": {"doc": each_date_obj.strftime("_%m/%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("_%m/%d/%y_")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m/%d/%y_")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%m-%d-%y_")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("_%#m-%d-%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("_%#m-%d-%y_")}},
                        # FOR those date with trailing of begining underscore

                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m/%d1/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m/%#d1/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%#d1/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%d1/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1/%d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1/%#d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1/%#d/%y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1/%d/%y")}},

                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m/%d1/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m/%#d1/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%#d1/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m/%d1/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1/%d/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%#m1/%#d/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1/%#d/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("%m1/%d/%Y")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("o%#m/o%#d/%2o15")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("o%#m/o%#d/%2o16")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("o%#m/o%#d/%2o17")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("o%#m/o%#d/%2o18")}},
                        # {"match_phrase": {"doc": each_date_obj.strftime("o%#m/o%#d/%2o19")}},

                        {"match_phrase": {"doc": each_date_obj.strftime("%ml%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m/%dl%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%m/%#dl%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("%#ml%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("l%#m/%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("l%#ml%d/%y")}},
                        {"match_phrase": {"doc": each_date_obj.strftime("l%#m/%dl%y")}},

                    ],

                    "minimum_should_match": "1"
                }
            },
            "q_level": 'DATE_FINDER',
            "corrected_date": each_date_obj.strftime("%m/%d/%Y"),
            "highlight": {
                "fields": {
                    "doc": {

                        "number_of_fragments": 0,
                        "type": "plain",
                        "fragmenter": "span"
                    }, },

                "post_tags": [
                    "~~"
                ],
                "pre_tags": [
                    "~~"
                ]
            }}

        list_of_queries.append(search_query)
        # exit()
    return list_of_queries