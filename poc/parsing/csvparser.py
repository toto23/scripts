import csv
from poc.parsing.fileInfos import FileInfos as FI


class CSVParser(FI):


    def __init__(self,infile):
        self.file = infile
        self.headers = self.__setHeaders__()
        self.headersTypes = self.__setFieldsTypes__()

    def __setHeaders__(self):
        # with open(self.file, encoding="UTF-8") as inf:
        with open(self.file) as inf:
            reader = csv.DictReader(inf, delimiter=';')
            headers = reader.fieldnames
        inf.close()
        return [x for x in headers if len(x) > 0]

    def __setFieldsTypes__(self):
        headersTypes = {}
        # with open(self.file, encoding="UTF-8") as inf:
        with open(self.file) as inf:
            values = "".join([next(inf) for x in range(1, 2)]).rstrip().split(';')
            for i in range(0,len(values)-1):
                values[i] = FI.cleanString(values[i])

                if      FI.isdate(str(values[i])):     headersTypes[self.headers[i]] = "Date"
                elif    FI.isint(str(values[i])):      headersTypes[self.headers[i]] = "Integer"
                elif    FI.isfloat(str(values[i])):    headersTypes[self.headers[i]] = "Float"
                else:                                  headersTypes[self.headers[i]] = "String"

        inf.close()
        return headersTypes

    def __setFieldsValues__(self):
        values = []
        # with open(self.file, encoding="UTF-8") as infile:
        with open(self.file) as infile:
            next(infile)
            row_data = {}
            for line in infile:
                # Remove empty line
                if not len(line.rstrip()) == 0:
                    # Remove extra ; char at the end of line
                    if line.rstrip().endswith((';')):
                        line = line.rstrip()[:-1]
                    for i, key in enumerate(self.headersTypes):
                        if key: row_data[key] = FI.cleanString(line.split(';')[i])
                    values.append(row_data)
            infile.close()
        return values

    def getHeadersTypes(self):
        return self.headersTypes

    def checkDocStruct(self):
        # with open(self.file, encoding="UTF-8") as infile:
        with open(self.file) as infile:
            next(infile)
            for line in infile:
                # Remove empty line
                if not len(line.rstrip())==0:
                    # Remove extra ; char at the end of line
                    if line.rstrip().endswith((';')):
                        line = line.rstrip()[:-1]
                    if len(line.rstrip().split(';')) != len(self.headersTypes):
                        print("Problem with file format with line")
                        print("Number of fields(" + str(
                            len(line.rstrip().split(';'))) + ") differs from number of headers(" + str(
                            len(self.headersTypes)) + "): ")
                        print(" ".join(line.rstrip().split(';')))
                        return False
                    else:
                        return True
        infile.close()

    def extractData(self):
        try:
            return self.__setFieldsValues__()
        except Exception as e:
            print("Unexpected error: " + str(e))
