import os.path
from dateutil.parser import parse
from ..technos.elastic import Elastic

class ParseFileInfos(object):

    _availableFormats = ["csv","xml","json","doc","pdf"]

    def __init__(self, fname):
        self._fname = str(fname)
        self._ftext = None

    @staticmethod
    def isint(value):
        try:
            int(value)
            return True
        except:
            return False

    @staticmethod
    def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

    @staticmethod
    def isdate(value):
        try: 
            if len(value) >= 4:    
                parse(value)
                return True
            else:
                return False
        except ValueError:
            return False

    def isvalid(self):
        try:
            if not os.path.isfile(self._fname):
                print("File does not exist")
                return False
            elif '.' not in str(self._fname):
                print("No file extension found")
                return False
            else:
                ext = str(self._fname).split(".")[-1:][0]
                if ext not in self._availableFormats:
                    print("Extension "+ext+" not recognized")
                    return False
                else:
                    self._ftext = ext
                    return True
        except Exception as e:
            print("Unexpected error: "+str(e))
            return False

    def get_extension(self):
        return self._ftext

    @staticmethod
    def _get_headers(infile):
        # with open(infile, encoding="ISO-8859-1") as inf:
        with open(infile, encoding="windows-1252") as inf:
            headers = inf.readline().rstrip().split(';')
            for f in headers:
                if f.startswith('"') and f.endswith('"'):
                    headers.__setitem__(headers.index(f), f[1:-1])
        inf.close()
        return headers

    @staticmethod
    def _get_types(headers, infile):
        fields_infos = {}
        # with open(infile, encoding="ISO-8859-1") as inf:
        with open(infile, encoding="windows-1252") as inf:
            values = "".join([next(inf) for x in range(1, 2)]).rstrip().split(';')
            for i in range(len(values)):
                if values[i].startswith('"') and values[i].endswith('"'):
                    values[i] = values[i][1:-1]
                if ParseFileInfos.isdate(str(values[i])):
                    fields_infos[headers[i]] = "Date"
                elif ParseFileInfos.isint(str(values[i])):
                    fields_infos[headers[i]] = "Integer"
                elif ParseFileInfos.isfloat(str(values[i])):
                    fields_infos[headers[i]] = "Float"
                else:
                    fields_infos[headers[i]] = "String"
        inf.close()
        return fields_infos

    def extract(self):
        if self._ftext.lower() == "csv":
            return self._extract_csv_infos(self._fname)
        elif self._ftext.lower() == "xml":
            self._extractXMLInfos(self._fname)
        elif self._ftext.lower() == "json":
            self._extractJSONInfos(self._fname)
        else:
            print(self._ftext+" format not implemented yet")

    def checkDocStruct(fields_infos, fname):
        with open(fname, encoding="windows-1252") as infile:
            next(infile)
            for line in infile:
                if len(line.rstrip().split(';')) != len(fields_infos):
                    print("Problem with file format")
                    print("Number of fields("+str(len(line.rstrip().split(';'))) +") differs from number of headers("+len(fields_infos)+"): ")
                    print(" ".join(line.rstrip().split(';')))
        infile.close()

    @staticmethod
    def _extract_csv_infos(infile):
        try:
            headers = ParseFileInfos._get_headers(infile)
            return ParseFileInfos._get_types(headers,infile)
        except Exception as e:
            print("Unexpected error: "+str(e))

    def _extractXMLInfos(self, infile):
        ''' to complete '''
        
    def _extractJSONInfos(self, infile):
        ''' to complete '''    