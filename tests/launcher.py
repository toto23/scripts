import sys, getopt, os, json
from poc.technos.elastic import Elastic as ES
from poc.parsing.csvparser import CSVParser as CSV
from poc.parsing.xmlparser import XMLParser as XML
from poc.parsing.fileInfos import FileInfos as FI

# global
global input_file, type_name, fields_infos


# functions
def main():
    try:
        options, values = getopt.getopt(sys.argv[1:], 'f:t:', ['file=', 'type_name='])
    except getopt.GetoptError as err:
        usage(err)
        sys.exit(1)
    for opt, arg in options:
        if opt in ('-f', '--file'):
            global input_file
            input_file = arg
        if opt in ('-t', '--type_name'):
            global type_name
            type_name = arg


def usage(err):
    print("Error: " + str(err))
    print("Usage: sys.argv[0] -f <file_name> -t <type_name>")


# main
if __name__ == "__main__":
    main()

file_info = FI(input_file)
if not file_info.isvalid():
    print("File " + input_file + " *** not valid ***")
    sys.exit(1)
else:
    print("Extracting file information structure...")
    index_name, extension = os.path.splitext(os.path.basename(input_file))
    # file_info.convertToUTF8(input_file)

    parser = None
    try:
        if extension.lower() == ".csv":
            parser = CSV(input_file)
            print("Checking doc structure...")
            if not parser.checkDocStruct():
                print("Checking structure ***KO***")
                sys.exit(1)
        elif extension.lower() == ".xml":
            parser = XML(input_file)
            print("Checking doc structure...")
            if not parser.checkDocStruct():
                print("Checking structure ***KO***")
                sys.exit(1)
        elif extension.lower() == ".json":
            print("JSON format not implemented yet")
            #fields_infos = JSON.extract(input_file)
            sys.exit(1)
        else:
            print(extension + " unknown format")
            sys.exit(1)

    except Exception as e:
        print("An error occured: " + str(e))
        sys.exit(1)

    try:
        print("Provisionning infrastucture...")
        #a ecrire

    except Exception as e:
        print("An error occured: " + str(e))
        sys.exit(1)

    try:
        print("Extracting data...")
        data = parser.extractData()

        print("Creating data bulk...")
        bulk = ES.buildBuffer(data)

        print(bulk)

    except Exception as e:
        print("An error occured: " + str(e))
        sys.exit(1)

    try:
        print("Creating template...")
        response = ES.setTemplate(type_name, parser.getHeadersTypes())
        if not response.ok:
            print("Failed! " + str(response.content))
            sys.exit(1)

    except Exception as e:
        print("An error occured: " + str(e))
        sys.exit(1)

    try:
        print("Pushing data...")
        response = json.loads(ES.pushData(type_name, bulk).text)
        if response.get("error"):
            print("*** KO ***")
            sys.exit(1)
        elif response.get("errors"):
            docKO = 0
            for action in response.get("items"):
                if action.get("index").get("status") != 200 and action.get("index").get("status") != 201:
                    docKO += 1
                    print("status: "+ str(action.get("index").get("status"))+" error: "+str(action.get("index").get("error")))
            print("*** KO: " + str(docKO) + " document(s) write(s) failed on "+str(len(response.get("items")))+" ***")
        else:
            print("OK: "+str(len(response.get("items")))+ " documents written")

    except Exception as e:
        print("An error occured: " + str(e))
        sys.exit(1)

print("End of job")
sys.exit(0)