import sys, getopt, os
from poc.parsing.parseFileInfos import ParseFileInfos
from poc.technos.elastic import Elastic

# global
global input_file, type_name

# functions
def main():
    try:
        options, values = getopt.getopt(sys.argv[1:], 'f:t:', ['file=','type_name='])
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
    print("Error: "+str(err))
    print("Usage: sys.argv[0] -f <file_name> -t <type_name>")
            
# main
if __name__ == "__main__":
    main()

parser = ParseFileInfos(input_file)
if not parser.isvalid():
    print("File "+input_file+" *** not valid ***")
    sys.exit(1)
else:
    try:
        print("Extracting file information structure...")
        fields_infos = parser.extract()
        index_name, extension = os.path.splitext(os.path.basename(input_file))

        print("Creating mapping...")
        Elastic.setMapping(index_name, type_name,fields_infos)

        # print("Creating doc structure...")
        # Elastic.createDocStruct(fields_infos)

        print("Checking doc structure...")
        if not ParseFileInfos.checkDocStruct(fields_infos, input_file):
            print("Checking structure ***KO***")

        print("Creating data bulk...")
        data = ParseFileInfos.createDocData(input_file)

        print("Pushing index settings...")
        Elastic.setIndexSettings(index_name)

        print("Pushing alias settings...")
        Elastic.setAlias(index_name)

        print("Pushing document mapping...")
        Elastic.setMapping(index_name)

        # print("Creating data bulk...")
        # Elastic.buildBuffer(fields_infos, input_file)

        print("Pushing data...")
        Elastic.pushData(index_name, data)

    except Exception as e:
        print("An error occured: "+str(e))
        sys.exit(1)

print("End of job")
sys.exit(0)