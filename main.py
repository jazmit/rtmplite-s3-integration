import  multitask, time, os, logging
from s3_help import Storage
from rtmp import FlashServer

log = logging.getLogger('__main__')

class App():
    """
    main app for schoolshape run rtmplite
    """
    
    def __init__(self, root="/videoStreams/", host="0.0.0.0", port=1935, logger="/var/log/rtmplite/main.log", s3config="/etc/.rtmplite-s3-integration"):
        self.root = root
        self.host = host
        self.port = port
        self.logger = logger
        self.s3config = s3config

    def run(self):
        self.initLogging()
        self.uploadRemain()
        self.startRtmp()

    def uploadRemain(self):
        for r,d,f in os.walk(self.root):
            for files in f:
                if files.endswith("flv"):
                    fullname = os.path.join(r, files)
                    remoteName = fullname[len(self.root):]
                    Storage(remoteName).upload(fullname)

    def startRtmp(self):
        try:
            Storage.loadConfig(self.s3config)
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
   App().run()
