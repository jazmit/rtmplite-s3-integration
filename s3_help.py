from ConfigParser import ConfigParser
from boto.s3.connection import S3Connection
import logging, os, time, sys, shutil

log = logging.getLogger('__main__')

class Storage():
    """
    operate s3 upload and download
    """
    localPath = "";
    s3Path = "";
    CONFIGFILE = None
    accessKey = None
    secretKey = None
    test = False
    
    def __init__(self, filename):
        self.filename = filename
        self.key = None
    
    @staticmethod 
    def loadConfig(configPath):
        if configPath is not None and Storage.CONFIGFILE!=configPath:
            Storage.CONFIGFILE = configPath
            config = ConfigParser()
            config.read(Storage.CONFIGFILE)
            Storage.accessKey = config.get("Credentials","aws_access_key_id")
            Storage.secretKey = config.get("Credentials","aws_secret_access_key")
            log.info('loading accessKey & secretKey from ' + configPath)

    def getLocalFileFullname(self):
        return Storage.localPath + self.filename
    
    def getBucketName(self):
        bucketName = 'private' if 'private' in self.filename else 'public'
        return bucketName + '.media.schoolshape.com'
    
    def getS3FileFullname(self):
        return Storage.s3Path + self.getBucketName() + r'/' + self.filename
        
    def startUpload(self):
        if Storage.test :
            #localhost mock s3upload
            shutil.copyfile(self.getLocalFileFullname(), self.getS3FileFullname())
            time.sleep(5)
        else :
            self.getKey().set_contents_from_filename(self.filename)
            self.closeKey()

    # def uploadByFile(self, fp, cb=None):
    #     self.getKey().set_contents_from_file(fp, rewind=True)
    #     if cb is not None and callable(cb): cb()
    #     self.closeKey()
    #     if not fp.closed: fp.close()

    def closeKey(self):
        if self.key is not None and not self.key.closed : self.key.close()
        self.key = None

    def getKey(self):
        if self.key is None:
            conn = S3Connection(Storage.accessKey, Storage.secretKey)
            bucket = conn.get_bucket(self.getBucketName())
            key = bucket.get_key(self.filename)
            if key is None: key = bucket.new_key(self.filename)
            self.key = key
        return self.key
    
    def tidyFileWithFfmpeg(self):
        localFileFullname = self.getLocalFileFullname()
        command = r'ffmpeg -metadata videocodecid="" -vn -acodec copy -y -i ' + localFileFullname + ' ' + localFileFullname
        os.system(command)

    def upload(self, failTimes=3, cb=None):
        """
        upload fp to S3 by filename and try 'failTimes' to upload if failed.
        
        Arguments:
        - `failTimes`: defaul try 3 times if upload failed
        - `cb`: callbackFun
        """
        log.info("start upload " + self.getLocalFileFullname() + " to " + self.getS3FileFullname())
        for i in range(failTimes):
            try:
                start = time.time()
                if self.filename.find(r'/audio/')>-1 and self.filename.endswith("flv"): self.tidyFileWithFfmpeg()
                self.startUpload()
                uploadedFullname = self.getLocalFileFullname()+'.uploaded'
                if os.path.isfile(uploadedFullname): os.remove(uploadedFullname)
                os.rename(self.getLocalFileFullname(), uploadedFullname)
                log.debug(('upload succeeded: ', self.filename, ', failed: ', i, ', spent: ', time.time()-start))
                if cb is not None and callable(cb): cb()
            except (Exception) , e:
                log.info(("upload failed: ", self.filename , ', failed: ', i, 'exception: ', e))
            else :
                break
        else:
            log.error(("upload failed: ", self.filename))
            
## just for test
if __name__=="__main__" :
    Storage.loadConfig('/etc/.rtmplite-s3-integration')
    print "accessKey:", Storage.accessKey,",secretKey:", Storage.secretKey
    s3 = Storage('vo/1.flv', test=True)
    s3.upload("/videoStreams/1.flv")
