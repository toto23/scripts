import os.path

class CheckFile(object):

    _availableFormats = ["csv","xml","json","doc","pdf"]

    def __init__(self, fname):
        self._fname = fname
                
    def isValid(self):
        try:
            if not os.path.isfile(self._fname):
                print "File does not exist"
                return False
            elif '.' not in str(self._fname):
                print "No file extension found"
                return False
            else:
                ext = str(str(self._fname).split(".")[-1:])[2:-2]
                if ext not in CheckFile._availableFormats:
                    print "Extension "+ext+" not recognized"
                    return False
                else:
                    self._fext = ext  
                    return True
        except Exception, e:
            print "Unexpected error: "+str(e)
            return False
        
    def getExtension(self):
        return self._fext    