from ConfigParser import ConfigParser
from boto.s3.connection import S3Connection

CONFIGFILE = "S3.ini"
config = ConfigParser()
config.read(CONFIGFILE)

accessKey = config.get("Credentials","aws_access_key_id")
secretKey = config.get("Credentials","aws_secret_access_key")
# conn = S3Connection(accessKey, secretKey)

class Storage():
    """
    operate s3 upload and download
    """
    
    def __init__(self, remoteFilename):
        self.remoteFilename = remoteFilename
        self.key = None
        self.test = False

    def uploadByFilename(self, localFilename, cb=None):
        print 'start upload!'
        self.getKey().set_contents_from_filename(localFilename)
        if cb is not None and callable(cb): cb()
        self.closeKey()
        print 'update ok!'


    def uploadByFile(self, fp, cb=None):
        self.getKey().set_contents_from_file(fp, rewind=True)
        if cb is not None and callable(cb): cb()
        self.closeKey()
        if not fp.closed: fp.close()

    def closeKey(self):
        if self.key is not None and not self.key.closed:
            self.key.close()
        self.key = None

    def getKey(self):
        def getBucketName():
            keyname = 'public'
            if self.remoteFilename.find('private')>-1: keyname = 'private'
            if self.test : keyname = 'test.' + keyname
            return keyname + '.media.schoolshape.com'
        if self.key is None:
            conn = S3Connection(accessKey, secretKey)
            bucket = conn.get_bucket(getBucketName())
            key = bucket.get_key(self.remoteFilename)
            if key is None: key = bucket.new_key(self.remoteFilename)
            self.key = key
        return self.key

if __name__=="__main__" :
    print "accessKey:", accessKey,",secretKey:", secretKey
    s3 = Storage('123.flv')
    s3.uploadByFilename("/vo/123.flv.flv")
