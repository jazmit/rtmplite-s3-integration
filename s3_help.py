from ConfigParser import ConfigParser
from boto.s3.connection import S3Connection
import logging, os

class Storage():
    """
    operate s3 upload and download
    """
    CONFIGFILE = None
    accessKey = None
    secretKey = None
    
    def __init__(self, remoteFilename, test=False):
        self.remoteFilename = remoteFilename
        self.key = None
        self.test = test
    
    @staticmethod 
    def loadConfig(configPath):
        if configPath is not None and Storage.CONFIGFILE!=configPath:
            Storage.CONFIGFILE = configPath
            config = ConfigParser()
            config.read(Storage.CONFIGFILE)
            Storage.accessKey = config.get("Credentials","aws_access_key_id")
            Storage.secretKey = config.get("Credentials","aws_secret_access_key")
            log.info('loading accessKey & secretKey from ' + configPath)

    def uploadByFilename(self, localFilename, cb=None):
        import time
        start = time.time()
        self.getKey().set_contents_from_filename(localFilename)
        if cb is not None and callable(cb): cb()
        self.closeKey()

        interTime = time.time() - start
        log.debug(('update file ', localFilename, ',waste time', interTime))

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
            conn = S3Connection(Storage.accessKey, Storage.secretKey)
            bucket = conn.get_bucket(getBucketName())
            key = bucket.get_key(self.remoteFilename)
            if key is None: key = bucket.new_key(self.remoteFilename)
            self.key = key
        return self.key

    def ffmpegTranscode(self, fromFile, toFile=None):
        if toFile is None: toFile = fromFile
        command = r'ffmpeg -metadata videocodecid="" -vn -acodec copy -y -i ' + fromFile + ' ' + toFile
        print 'transcode',command

        os.system(command)

    def upload(self, fn, failTimes=3, cb=None):
        """
        upload fp to S3 by filename and try 'failTimes' to upload if failed.
        
        Arguments:
        - `fn`: filename.
        - `failTimes`: defaul try 3 times if upload failed
        - `cb`: callbackFun
        """
        for i in range(failTimes):
            try:
                if self.remoteFilename.find(r'/audio/')>-1 and fn.endswith("flv"):
                    self.ffmpegTranscode(fn)
                self.uploadByFilename(fn, cb)
                os.rename(fn, fn+'.uploaded')
            except (Exception) , e:
                log.info(("upload failed,", fn , ",try Times:", i, 'Exception:', e))
            else :
                break
        else:
            log.info(("upload failed,", fn))

log = logging.getLogger('__main__')
Storage.loadConfig('S3.ini')

if __name__=="__main__" :
    Storage.loadConfig('/etc/.rtmplite-s3-integration')
    print "accessKey:", Storage.accessKey,",secretKey:", Storage.secretKey
    s3 = Storage('/vo/audio/1.flv', test=True)
    s3.upload("/vo/test/1.flv")
