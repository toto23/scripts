import requests
import datetime
import json

class Elastic(object):
    
    # Pour le mapping des donnees
    MapHeader = " \"mappings\": { \"TYPE_NAME\": { \"properties\": { "
    # MapHeader = "{ \"TYPE_NAME\": { \"properties\": { "
    #MapFooter = " } } } }"
    MapFooter = " } } }"
    # MapStringField = "\"FIELD_NAME\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } }"
    MapStringField = "\"FIELD_NAME\": { \"type\": \"text\" }"
    MapDateField = " \"FIELD_NAME\": { \"type\": \"date\", \"format\": \"strict_date_optional_time||epoch_millis\" }"
    MapLongField = " \"FIELD_NAME\": { \"type\": \"long\" }"
    MapFloatField = " \"FIELD_NAME\": { \"type\": \"float\" }"
    # MapDynamicStringField =  " \"match_mapping_type\": \"string\", \"mapping\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } } } }"
    MapDynamicStringField =  " \"match_mapping_type\": \"string\", \"mapping\": { \"type\": \"text\" } } }"

    # Pour le settings des indices
    IndexSettings = "{ \"index_patterns\": [ \"PATTERN_LIST\" ], \"settings\" : { \"number_of_shards\" : 1, \"number_of_replicas\" : 0 }"
    # IndexSettings = "{ \"index_patterns\": \"PATTERN_LIST\", \"settings\" : { \"index\" : { \"number_of_shards\" : 3, \"number_of_replicas\" : 2 } }"
    IndexAnalysers = ""
    # IndexAlias = "{ \"aliases\" : { \"ALIAS_NAME\": { \"filter\": { \"term\": { \"FIELD\": \"VALUE\"} }, \"routing\": \"VALUE\"} } }"
    IndexAlias = "\"aliases\" : { \"ALIAS_NAME\": { } } }"

    @staticmethod
    def getIndexSettings(type_name):
        return Elastic.IndexSettings.replace("PATTERN_LIST",type_name+"*")

    @staticmethod
    def getAlias(type_name):
        return Elastic.IndexAlias.replace("ALIAS_NAME",type_name+"View")

    @staticmethod
    def getMapping(type, fields_infos):
        mapping = [Elastic.MapHeader.replace("TYPE_NAME", type)]
        for i, name in enumerate(fields_infos):

            if      fields_infos.get(name) == "Date":       mapping.append(Elastic.MapDateField.replace("FIELD_NAME", name))
            elif    fields_infos.get(name) == "Integer":    mapping.append(Elastic.MapLongField.replace("FIELD_NAME", name))
            elif    fields_infos.get(name) == "Float":      mapping.append(Elastic.MapFloatField.replace("FIELD_NAME", name))
            elif    fields_infos.get(name) == "String":     mapping.append(Elastic.MapStringField.replace("FIELD_NAME", name))

            if i != len(fields_infos) - 1:
                mapping.append(",")
            else:
                mapping.append(Elastic.MapFooter)

        return ''.join(mapping)

    @staticmethod
    def setTemplate(type_name, headers_types):
        settings = Elastic.getIndexSettings(type_name)
        mapping = Elastic.getMapping(type_name, headers_types)
        alias = Elastic.getAlias(type_name)

        return Elastic.putRequest("/_template/tpl_"+type_name, settings+","+mapping+","+alias)

    @staticmethod
    def pushData(type_name, data):
        now = datetime.datetime.now()
        return Elastic.postRequest("/"+type_name+"."+str(now.year)+"-"+str(now.month)+"-"+str(now.day)+"/"+type_name+"/_bulk", data)

    @staticmethod
    def putRequest(url, data):
        base_url = "http://elastic02.westeurope.cloudapp.azure.com:9200"
        # base_url = "http://localhost:9200"

        return requests.put(base_url+url, headers={'Content-Type':'application/json;charset=UTF-8"'}, data=data)

    @staticmethod
    def postRequest(url, data):
        base_url = "http://elastic01.westeurope.cloudapp.azure.com:9200"
        # base_url = "http://localhost:9200"

        return requests.post(base_url+url, headers={'Content-Type':'application/json;charset=UTF-8"'}, data=data)

    def buildBuffer(data):
        sb = []
        for row in data:
            sb.append("{ \"index\" : { } }\n")
            # sb.append("{ \"doc\": { ")
            sb.append("{ ")
            for i, key in enumerate(row):
                if key:
                    sb.append("\"" + key + "\": \"" + row.get(key) + "\"")
                if i != len(row) - 1:
                    sb.append(",")
                else:
                    # sb.append("} }\n")
                    sb.append("}\n")
        return ''.join(sb)

    def buildResponse(response):
        res = json.loads(response.text)
        if res.get("errors"):
            docKO = 0
            for action in res.get("items"):
                if action.get("index").get("status") != 200 and action.get("index").get("status") != 201:
                    docKO += 1
                    print("status: "+ str(action.get("index").get("status"))+" error: "+str(action.get("index").get("error")))
            print("*** KO: " + str(docKO) + " document(s) write(s) failed on "+str(len(dict(res).get("items")))+" ***")
        else:
            print("OK: "+str(len(dict(res).get("items"))+ " documents written"))