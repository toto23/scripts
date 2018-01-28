from xml.dom import minidom
from poc.parsing.fileInfos import FileInfos as FI


class XMLParser(FI):


    def __init__(self,infile):
        self.file = infile
        self.dom = minidom.parse(infile)
        self.root = self.dom.documentElement
        self.type = None
        self.headers = self.__setHeaders__(self.root, self.root, [])
        self.headersTypes = self.__setFieldsTypes__()

    def __setHeaders__(self, root, node, result):
        if node.hasChildNodes():
            for n in node.childNodes:
                if n.nodeType == n.ELEMENT_NODE:
                    if n.parentNode.nodeName != root.tagName:
                        result.append(str(n.nodeName))
                    else:
                        self.type = n.nodeName
                    self.__setHeaders__(root, n, result)

        return set(result)

    def __setFieldsValues__(self):
        values = []
        for i in range(0, len(self.dom.getElementsByTagName(self.type))):
            row_data = {}
            for f in self.headers:
                el = self.dom.getElementsByTagName(f)
                if el[i].hasChildNodes():
                    row_data[f] = el[i].firstChild.nodeValue
            values.append(row_data)
        return values

    def __setFieldsTypes__(self):
        headers_types = {}
        for f in self.headers:
            el = self.dom.getElementsByTagName(f)
            if el[0].hasChildNodes():
                value = el[0].firstChild.nodeValue
                if FI.isdate(FI.cleanString(value)):
                    headers_types[f] = "Date"
                elif FI.isint(FI.cleanString(value)):
                    headers_types[f] = "Integer"
                elif FI.isfloat(FI.cleanString(value)):
                    headers_types[f] = "Float"
                else:
                    headers_types[f] = "String"
            else:
                headers_types[f] = "String"
        return headers_types

    def getHeadersTypes(self):
        return self.headersTypes

    def checkDocStruct(self):
        count = 0
        if self.root.hasChildNodes():
            for n in self.root.childNodes:
                if n.nodeType == n.ELEMENT_NODE:
                    for k in n.childNodes:
                        if k.nodeType != k.TEXT_NODE:   count += 1

                    if count == len(self.headersTypes): count = 0
                    else:
                        print("node: "+n.nodeName)
                        print("Number of nodes(" + str(count) + ") differs from number of headers(" + str(
                            len(self.headersTypes)) + "): ")
                        return False
        return True

    def extractData(self):
        try:
            return self.__setFieldsValues__()
        except Exception as e:
            print("Unexpected error: " + str(e))