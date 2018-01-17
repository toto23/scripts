import sys, getopt
from tests.extractFileInfos import ExtractFileInfos
from tests.checkFile import CheckFile

# global
global input_file, type_name

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
    print "Error: "+str(err)
    print "Usage: sys.argv[0] -f <file_name> -t <type_name>"
            
# main
if __name__ == "__main__":
    main()

cf = CheckFile(input_file)
if not cf.isValid():
    print "File "+input_file+" *** not valid ***"
    sys.exit(1)
else:
    print "Extracting file information structure..."
    efi = ExtractFileInfos(input_file, cf.getExtension())
    efi.extract()
    print "Creating mapping..."
    efi.createMapping(type_name)

## Push mapping + index settings to es
## Push data

print "End of job"
sys.exit(0)