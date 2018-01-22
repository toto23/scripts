from elasticsearch import Elasticsearch, helpers
import csv
from elasticsearch.connection import RequestsHttpConnection

class Elastic(object):
    
    # Pour le mapping des donnees
    MapHeader = "{ \"mappings\": { \"TYPE_NAME\": { \"properties\": { "
    MapFooter = " } } } }"
    MapStringField = "\"FIELD_NAME\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } }"
    MapDateField = " \"FIELD_NAME\": { \"type\": \"date\", \"format\": \"strict_date_optional_time||epoch_millis\" }"
    MapLongField = " \"FIELD_NAME\": { \"type\": \"long\" }"
    MapFloatField = " \"FIELD_NAME\": { \"type\": \"float\" }"
    MapDynamicStringField =  " \"match_mapping_type\": \"string\", \"mapping\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } } } }"

    # Pour le settings des indices
    IndexSettings = " { \"settings\" : { \"index\" : { \"number_of_shards\" : 3, \"number_of_replicas\" : 2 } } "
    IndexAnalysers = ""
    # IndexAlias = "{ \"aliases\" : { \"ALIAS_NAME\": { \"filter\": { \"term\": { \"FIELD\": \"VALUE\"} }, \"routing\": \"VALUE\"} } }"
    IndexAlias = "{ \"aliases\" : { \"ALIAS_NAME\": { } } }"

    @staticmethod
    def getIndexSettings():
        return Elastic.IndexSettings

    @staticmethod
    def getAlias(alias_name):
        return Elastic.IndexAlias.replace("ALIAS_NAME",alias_name)

    @staticmethod
    def getMapping(type, fields_infos):
        with open("../output/"+str(type)+"_mapping.json", "a") as outfile:
            outfile.write(Elastic.MapHeader.replace("TYPE_NAME", type))
            for i, name in enumerate(fields_infos):
                if fields_infos.get(name) == "Date":
                    outfile.write(Elastic.MapDateField.replace("FIELD_NAME", name))
                elif fields_infos.get(name) == "Integer":
                    outfile.write(Elastic.MapLongField.replace("FIELD_NAME", name))
                elif fields_infos.get(name) == "Float":
                    outfile.write(Elastic.MapFloatField.replace("FIELD_NAME", name))
                elif fields_infos.get(name) == "String":
                    outfile.write(Elastic.MapStringField.replace("FIELD_NAME", name))
                if i != len(fields_infos)-1:
                    outfile.write(",")
                else:
                    outfile.write(Elastic.MapFooter)
        outfile.close()

    @staticmethod
    def setIndexSettings(index_name):
        data = Elastic.getIndexSettings()
        Elastic.sendRequest("/"+index_name+"/_settings", data)

    @staticmethod
    def setAlias(index_name, type_name):
        data = { "actions": [ { "add": { "index": ""+index_name+"", "alias": ""+Elastic.getAlias(type_name+"View")+""} } ] }
        Elastic.sendRequest("/"+index_name+"/_aliases", data)

    @staticmethod
    def setMapping(index_name, type_name, fields_infos):
        data = Elastic.getMapping(type_name, fields_infos)
        Elastic.sendRequest("/"+index_name+"/_mapping", data)

    @staticmethod
    def pushData(index_name, data):
        Elastic.sendRequest("/"+index_name+"/_bulk", data)

    @staticmethod
    def sendRequest(url, data):

        # self, host = 'localhost', port = 9200, use_ssl = False, url_prefix = '', timeout = 10, ** kwargs
        # es_conn = RequestsHttpConnection("elastic-test-01.westeurope.cloudapp.azure.com",9200)
        es_conn = RequestsHttpConnection("52.174.36.247",9200)

        # self, method, url, params = None, body = None, timeout = None, ignore = (), headers = None
        code, headers, raw_data = es_conn.perform_request("POST", url, None, data, 5, (), None)
        print("code: "+code+" headers: "+headers+" raw_data: "+raw_data)
        if code != 200 or code != 201:
            print("Push data failed")

        # helpers.bulk(Elastic.es,data)

        # helpers.bulk(es, k)
        #(2650, [])

        # check to make sure we got what we expected...
        #es.count(index='test')
        #{u'count': 2650, u'_shards': {u'successful': 1, u'failed': 0, u'total': 1}}
        #res = es.index(index="test-index", doc_type='tweet', id=1, body=values)
        #print(res['created'])
        #data = urllib.urlencode(data)
        #req = urllib.Request(self._url, data, self._headers)
        #response = urllib.urlopen(req)
        #the_page = response.read()

    # def createDocData(fields_infos, values):
    #     with open("../output/data.json", "a") as outfile:
    #         outfile.write("{ \"doc\": { ")
    #         for i, key in enumerate(fields_infos):
    #             if key:
    #                 # print("\""+key+"\": \""+values[i].rstrip()+"\" => idx: "+str(i)+" for values size: "+str(len(values)))
    #                 # print(" ".join(values))
    #                 # print("\""+key+"\": \""+values[i].replace('\r', ''))
    #                 outfile.write("\""+key+"\": \""+values[i].rstrip()+"\"")
    #             if i != len(fields_infos)-1:
    #                 outfile.write(",")
    #         outfile.write(" } }\n")
    #     outfile.close()

    def createDocData(fields_infos, values):
        sb = []
        sb.append("{ \"doc\": { ")
        for i, key in enumerate(fields_infos):
            if key:
                sb.append("\"" + key + "\": \"" + values[i].rstrip() + "\"")
            if i != len(fields_infos) - 1:
                sb.append(",")
            else:
                sb.append("\n")
        return ''.join(sb)

    def buildBuffer(fields_infos,fname):
        sb = []
        with open(fname, encoding="windows-1252") as infile:
            next(infile)
            for line in infile:
                sb.append(Elastic.createDocData(fields_infos, line.rstrip().split(';')))
        infile.close()
        return ''.join(sb)