import  multitask, time, os, logging
from s3_help import Storage
from rtmp import FlashServer

log = logging.getLogger('__main__')

class App():
    """
    main app for schoolshape run rtmplite
    """
    
    def __init__(self, root=r"/videoStreams/", host="0.0.0.0", port=1935, logger=r"/var/log/rtmplite/main.log", s3config=r"/etc/.rtmplite-s3-integration", s3Path="", test=False):
        self.root = root
        self.host = host
        self.port = port
        self.logger = logger
        self.s3config = s3config
        self.s3Path = s3Path
        self.test = test
        
    def run(self):
        self.initLogging()
        Storage.test = self.test
        Storage.localPath = self.root
        Storage.s3Path = self.s3Path
        Storage.loadConfig(self.s3config)
        self.uploadRemain()
        self.startRtmp()

    #TODO
    def uploadRemain(self):
        for r,d,f in os.walk(self.root):
            for files in f:
                if files.endswith("flv"):
                    fullname = os.path.join(r, files)
                    filename = fullname[len(self.root):]
                    Storage(filename).upload()

    def startRtmp(self):
        try:
            agent = FlashServer()
            agent.root = self.root
            agent.start(self.host, self.port)
            log.info(( time.asctime() , 'Flash Server Starts - %s:%d' % (self.host, self.port)))
            multitask.run()
        except KeyboardInterrupt:
            pass
        log.info((time.asctime() ,'Flash Server Stops'))

    def initLogging(self):
        #create a logger
        logger = logging.getLogger('__main__')
        logger.setLevel(logging.DEBUG)
        #create a handler to write log
        fh = logging.FileHandler(self.logger)
        fh.setLevel(logging.INFO)
        # create a handler to output stdout
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        #init the formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
    
        logger.addHandler(ch)
        logger.addHandler(fh)

if __name__=="__main__":
    import sys
    if sys.platform.find("win")==-1 :
        App().run()
    else :
        #localhost mock s3VideoStreams
        rtmplitePath = r"D://schoolshape/rtmplite/"
        mockS3Path = r"D://schoolshape/s3VideoStreams/"
        App(root=rtmplitePath+r"videoStreams/", logger=rtmplitePath+"main.log", s3config=rtmplitePath+"S3.ini", s3Path=mockS3Path, test=True).run()
