# {
#   "foo": {
#     "type" "text",
#     "fields": {
#       "keyword": {
#         "type": "keyword",
#         "ignore_above": 256
#       },
#       "english": { 
#         "type":     "text",
#         "analyzer": "english"
#       }
#     }
#   }
# }

# {
#   "match_mapping_type": "string",
#   "mapping": {
#     "type": "text"
#   }
# }

# {
#     "aliases" : {
#         "alias_1" : {},
#         "alias_2" : {
#             "filter" : {
#                 "term" : {"user" : "kimchy" }
#             },
#             "routing" : "kimchy"
#         }
#     }
# }
# 
# {
#     "settings" : {
#         "index" : {
#             "number_of_shards" : 3,
#             "number_of_replicas" : 2
#         }
#     }
# }

class Elasticsearch(object):
    
    # Pour le mapping des donnees
    Header = "{ \"mappings\": { \"TYPE_NAME\": { \"properties\": { "
    Footer = " } } } }"
    StringField = "\"STRING_FIELD_NAME\": { \"type\": \"text\", \"analyzer\": \"whitespace\" }"
    DateField = " \"DATE_FIELD_NAME\": { \"type\": \"date\", \"format\": \"strict_date_optional_time||epoch_millis\" }"
    NumericField = " \"NUMERIC_FIELD_NAME\": { \"type\": \"float\" }"
    
    # Pour le settings des indices
    Index = ""
    Alias = ""
    
    # Prevoir methode connexion a ES    
    def __init__(self, url):
        self._url = url
        
    def sendData(self,fname):
        ''' to complete '''
        
    def buildBulk(self):
        ''' to complete '''
    
    
    
    