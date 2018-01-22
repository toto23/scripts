import sys, getopt, os, csv
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

# with open(input_file, encoding="ISO-8859-1") as csvfile:
#     reader = csv.DictReader(csvfile)
#     snif=csv.Sniffer()
#     if snif.has_header(csvfile.readline()):
#         fields= reader.fieldnames
#         print(str(fields))
#
# print("End of job")
# sys.exit(0)

# sniff_range = 4096
# delimiters = ';'
#
# sniffer = csv.Sniffer()
#
# print("Reading file: "+input_file)
#
# with open(input_file, encoding="ISO-8859-1") as infile:
#     # Determine dialect
#     dialect = sniffer.sniff(
#         infile.read(sniff_range), delimiters=delimiters
#     )
#     infile.seek(0)
#
#     # Sniff for header
#     has_header = sniffer.has_header(infile.read(sniff_range))
#     infile.seek(0)
#
#     #reader=csv.reader(infile, dialect)
#     reader=csv.DictReader(infile)
#     fields=reader.fieldnames
#     print(str(fields))
# infile.close()
#
# print("End of job")
# sys.exit(0)


parser = ParseFileInfos(input_file)
if not parser.isvalid():
    print("File "+input_file+" *** not valid ***")
    sys.exit(1)
else:
    try:
        print("Extracting file information structure...")
        fields_infos = parser.extract()

        print("Creating mapping...")
        Elastic.createDocMapping(type_name,fields_infos)

        # print("Creating doc structure...")
        # Elastic.createDocStruct(fields_infos)

        print("Checking doc structure...")
        ParseFileInfos.checkDocStruct(fields_infos, input_file)

        index_name, extension = os.path.splitext(os.path.basename(input_file))

        print("Pushing index settings...")
        Elastic.pushIndexSettings(index_name)

        print("Pushing alias settings...")
        Elastic.pushAlias(index_name)

        print("Pushing document mapping...")
        Elastic.pushAlias(index_name)

        print("Creating data bulk...")
        Elastic.buildBulkBuffer(fields_infos, input_file)

        print("Pushing data...")
        Elastic.pushData(index_name)
    except Exception as e:
        print("An error occured: "+str(e))
        sys.exit(1)

## Push mapping + index settings to es
## Push data

print("End of job")
sys.exit(0)