from dateutil.parser import parse
from tests.elasticsearch import Elasticsearch

class ExtractFileInfos(object):

    def __init__(self, fpath, ftype):
        self._fpath = fpath
        self._ftype = ftype
        
    def isLong(self,value):
        try:
            long(value)
            return True
        except:
            return False        
    
    def isFloat(self,value):
        try:
            float(value)
            return True
        except:
            return False    
    
    def isDate(self,value):
        try: 
            if len(value) >= 4:    
                parse(value)
                return True
            else:
                return False
        except ValueError:
            return False    
        
    def createMapping(self, type_name):
        with open("output/Elasticsearch.json", "a") as outfile:
            outfile.write(Elasticsearch.Header.replace("TYPE_NAME", type_name))
            for i in range(len(self._fnames)):
                if self._ftypes[i] == "Date":
                    outfile.write(Elasticsearch.DateField.replace("DATE_FIELD_NAME", self._fnames[i]))
                elif self._ftypes[i] == "Numeric":
                    outfile.write(Elasticsearch.NumericField.replace("NUMERIC_FIELD_NAME", self._fnames[i]))
                elif self._ftypes[i] == "String":
                    outfile.write(Elasticsearch.StringField.replace("STRING_FIELD_NAME", self._fnames[i]))
                if i != len(self._fnames)-1:
                    outfile.write(",")
                else:
                    outfile.write(Elasticsearch.Footer)   
    
    def extract(self):
        if self._ftype.lower() == "csv":
            self._extractCSVInfos(self._fpath)
        elif self._ftype.lower() == "xml":
            self._extractXMLInfos(self._fpath)
        elif self._ftype.lower() == "json":
            self._extractJSONInfos(self._fpath)    
        else:
            print self._ftype+" format not implemented yet"    

    def _extractCSVInfos(self, infile):
        try:
            self._fnames = list()
            self._ftypes = list()
            with open(infile) as inf:
                headers = "".join([next(inf) for x in xrange(1)]).rstrip()
                fields = headers.split(';')
                for f in fields:
                    if f.startswith('"') and f.endswith('"'):
                        f = f[1:-1]
                    self._fnames.append(f)
        
                types = "".join([next(inf) for x in xrange(1,2)]).rstrip()
                values = types.split(';')
                if len(values) != len(fields):
                    print "Nb de headers ("+len(fields)+") differents du nombre de valeurs ("+len(values)+")",
                else:
                    for v in values:
                        if v.startswith('"') and v.endswith('"'): 
                            v = v[1:-1]
                        if self.isDate(v): 
                            self._ftypes.append("Date")
                        elif self.isFloat(v): 
                            self._ftypes.append("Numeric")
                        else: 
                            self._ftypes.append("String")
        except Exception, e:
            print "Unexpected error: "+str(e)                            
    
    def _extraXMLInfos(self, infile):
        ''' to complete '''
        
    def _extraJSONInfos(self, infile):
        ''' to complete '''    