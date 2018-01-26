import requests, json
from elasticsearch.connection import RequestsHttpConnection

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
    IndexSettings = "{ \"index_patterns\": [ \"PATTERN_LIST\" ], \"settings\" : { \"number_of_shards\" : 3, \"number_of_replicas\" : 2 }"
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
        mapping = []
        mapping.append(Elastic.MapHeader.replace("TYPE_NAME", type))
        for i, name in enumerate(fields_infos):
            if fields_infos.get(name) == "Date":
                mapping.append(Elastic.MapDateField.replace("FIELD_NAME", name))
            elif fields_infos.get(name) == "Integer":
                mapping.append(Elastic.MapLongField.replace("FIELD_NAME", name))
            elif fields_infos.get(name) == "Float":
                mapping.append(Elastic.MapFloatField.replace("FIELD_NAME", name))
            elif fields_infos.get(name) == "String":
                mapping.append(Elastic.MapStringField.replace("FIELD_NAME", name))
            if i != len(fields_infos) - 1:
                mapping.append(",")
            else:
                mapping.append(Elastic.MapFooter)

        return ''.join(mapping)

    @staticmethod
    def setIndexSettings(index_name):
        data = Elastic.getIndexSettings()
        Elastic.putRequest("/"+index_name+"/_settings", data)

    @staticmethod
    def setAlias(index_name, type_name):
        data = { "actions": [ { "add": { "index": ""+index_name+"", "alias": ""+Elastic.getAlias(type_name+"View")+""} } ] }
        Elastic.sendRequest("/"+index_name+"/_aliases", data)

    @staticmethod
    def setMapping(index_name, type_name, fields_infos):
        data = Elastic.getMapping(type_name, fields_infos)
        Elastic.putRequest("/"+index_name+"/"+type_name+"/_mapping", data)

    @staticmethod
    def getTemplate(index_name, type_name, fields_infos):
        settings = Elastic.getIndexSettings(type_name)
        mapping = Elastic.getMapping(type_name, fields_infos)
        alias = Elastic.getAlias(type_name)

        # print("alias: "+alias)

        return settings+","+mapping+","+alias

    @staticmethod
    def setTemplate(index_name, type_name, fields_infos):
        data = Elastic.getTemplate(index_name,type_name, fields_infos)
        Elastic.putRequest("/_template/tpl_"+type_name, data)

    @staticmethod
    def pushData(index_name, data):
        Elastic.postRequest("/"+index_name+"/_bulk", data)

    @staticmethod
    def putRequest(url, data):
        base_url = "http://elastic02.westeurope.cloudapp.azure.com:9200"

        print("url: "+str(base_url+url))
        print("data: "+data)

        # d = json.loads(data)
        # d = json.dumps(data)

        response = requests.put(base_url+url, headers={'Content-Type':'application/json'}, data=data)
        results = json.loads(response.text)
        print(results)

    @staticmethod
    def postRequest(url, data):

        # self, host = 'localhost', port = 9200, use_ssl = False, url_prefix = '', timeout = 10, ** kwargs
        # es_conn = RequestsHttpConnection("elastic-test-01.westeurope.cloudapp.azure.com",9200)
        # es_conn = RequestsHttpConnection("elastic02.westeurope.cloudapp.azure.com",9200)

        # self, method, url, params = None, body = None, timeout = None, ignore = (), headers = None
        # code, headers, raw_data = es_conn.perform_request("POST", url, None, data, 5, (), None)
        # print("code: "+code+" headers: "+headers+" raw_data: "+raw_data)
        # if code != 200 or code != 201:
        #     print("Push data failed")

        base_url = "http://elastic02.westeurope.cloudapp.azure.com:9200"

        print("url: "+str(base_url+url))
        print("data: "+str(data))
        body=json.loads(data)

        response = requests.post(base_url+url, headers={'Content-Type':'application/json'}, data=json.loads(data))
        print(str(response.content))

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
        # with open(fname, encoding="windows-1252") as infile:
        # with open(fname, encoding="ISO-8859-1") as infile:
        with open(fname,encoding="UTF-8") as infile:
            next(infile)
            for line in infile:
                sb.append(Elastic.createDocData(fields_infos, line.rstrip().split(';')))
        infile.close()
        return ''.join(sb)