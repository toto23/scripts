from elasticsearch import Elasticsearch, helpers

class Elastic(object):
    
    # Pour le mapping des donnees
    MapHeader = "{ \"mappings\": { \"TYPE_NAME\": { \"properties\": { "
    MapFooter = " } } } }"
    MapStringField = "\"FIELD_NAME\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } }"
    #StringField = "\"STRING_FIELD_NAME\": { \"type\": \"text\", \"analyzer\": \"whitespace\" }"
    MapDateField = " \"FIELD_NAME\": { \"type\": \"date\", \"format\": \"strict_date_optional_time||epoch_millis\" }"
    MapLongField = " \"FIELD_NAME\": { \"type\": \"long\" }"
    MapFloatField = " \"FIELD_NAME\": { \"type\": \"float\" }"
    MapDynamicStringField =  " \"match_mapping_type\": \"string\", \"mapping\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256}, \"english\": { \"type\": \"text\", \"analyzer\": \"english\"} } } } }"

    # Pour le settings des indices
    IndexSettings = " \"settings\" : { \"index\" : { \"number_of_shards\" : 3, \"number_of_replicas\" : 2 } } "
    IndexAnalysers = ""
    IndexAlias = "{ \"aliases\" : { \"ALIAS_NAME\": { \"filter\": { \"term\": { \"FIELD\": \"VALUE\"} }, \"routing\": \"VALUE\"} } }"

    es = None

    # Prevoir methode connexion a ES
    def __init__(self, url):
        es = Elasticsearch(url)

        self._header = {'Content-Type', 'application/json'}

    def sendData(data):

        values = {'name': 'Michael Foord',
                  'location': 'Northampton',
                  'language': 'Python'}
        helpers.bulk(Elastic.es,data)

        # helpers.bulk(es, k)
        #(2650, [])
        # check to make sure we got what we expected...
        #es.count(index='test')
        #{u'count': 2650, u'_shards': {u'successful': 1, u'failed': 0, u'total': 1}}
        #res = es.index(index="test-index", doc_type='tweet', id=1, body=values)
        #print(res['created'])

#        data = urllib.urlencode(data)
#        req = urllib.Request(self._url, data, self._headers)
#        response = urllib.urlopen(req)
#        the_page = response.read()

    def createDocMapping(type, fields_infos):
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

    def createDocStruct(fields_infos):
        with open("../output/doc_template.json", "a") as outfile:
            outfile.write("{ ")
            for i, key in enumerate(fields_infos):
                outfile.write("\""+key+"\": \"VALUE"+str(i)+"\"")
                if i != len(fields_infos)-1:
                    outfile.write(",")
            outfile.write(" }")
        outfile.close()

    def buildBulkBuffer(fname, fields_infos):
        with open(fname, encoding="ISO-8859-1") as infile:
            values = "".join([next(infile) for x in range(1, 2)]).rstrip().split(';')




        with open("../output/"+str(type)+"_doc_template.json", "a") as outfile:
            # for line in fname:
            #     values = "".join([next(outfile) for x in range(1,2)]).rstrip().split(';')
            #
            #     outfile.write("{")
            #     for i in len(fields_infos):
            #         outfile.write("fields_infos[i]"+":"+"\"values[i]\"")
            #     outfile.write("}")
            #
            # for k in fields_infos.keys():
            #     outfile.write("\"+k\":"")
            #
            # outfile.write("}")



            fields = ('remote_addr', 'timestamp', 'url', 'status')
            #values = (remote_addr, timestamp, url, status)

            # We return a dict holding values from each line
            #es_nginx_d = dict(zip(es_fields_keys, es_fields_vals))

            # Return the row on each iteration
            #yield idx, es_nginx_d  # <- Note the usage of 'yield'
