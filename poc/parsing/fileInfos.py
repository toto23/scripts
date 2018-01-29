import os, os.path, codecs
from chardet.universaldetector import UniversalDetector
from dateutil.parser import parse


class FileInfos(object):

    AVAILABLE_FORMATS = ["csv","xml","json","doc","pdf"]

    def __init__(self, file_name):
        self.fname = file_name
        self.ftext = None

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
            if not os.path.isfile(self.fname):
                print("File does not exist")
                return False
            elif '.' not in str(self.fname):
                print("No file extension found")
                return False
            else:
                ext = str(self.fname).split(".")[-1:][0]
                #if ext not in self._availableFormats:
                if ext not in FileInfos.AVAILABLE_FORMATS:
                    print("Extension "+ext+" not recognized")
                    return False
                else:
                    self.ftext = ext
                    return True
        except Exception as e:
            print("Unexpected error: "+str(e))
            return False

    @staticmethod
    def cleanString(s):
        if s:
            s = s.lstrip().rstrip()
            while s.startswith('\"'):   s = s[1:]
            while s.endswith('\"'):     s = s[:-1]
            while s.startswith('\''):  s = s[1:]
            while s.endswith('\''):    s = s[:-1]

        return s

    @staticmethod
    def convertToUTF8(filename, out_enc='utf-8'):
        (filepath, name) = os.path.split(filename)
        try:
            f = open(filename, 'rb')
            b = b' '
            b += f.read(1024)

            u = UniversalDetector()
            u.reset()
            u.feed(b)
            u.close()

            f.seek(0)

            b = f.read()
            f.close()

            in_enc = u.result['encoding']

            if 'utf-8' != in_enc:
                new_content = b.decode(in_enc, 'ignore')
                f = open(filename, 'w', encoding=out_enc)
                f.write(new_content)
                f.close()
            #print('Success:' + filename + ' converted from ' + in_enc + ' to ' + out_enc)
        except IOError:
            print('Error:' + filename + ' failed to convert from ' + in_enc + ' to ' + out_enc)
        finally:
            f.close()


    # @staticmethod
    # def convertToUTF8(fname):
    #     BLOCKSIZE = 1048576 # or some other, desired size in bytes
    #     with codecs.open(fname, "r") as sourceFile:
    #         with codecs.open("data/file.utf8", "w", "utf-8") as targetFile:
    #             while True:
    #                 contents = sourceFile.read(BLOCKSIZE)
    #                 if not contents:
    #                     break
    #                 targetFile.write(contents)