from org.apache.hadoop.hbase import HBaseTestingUtility

class HBaseContext(HBaseTestingUtility):
    def __init__(self):
        super(HBaseContext, self).__init__()

        #if self.conf.get('test.build.data') is None:
        #    self.conf.set('test.build.data', '/tmp/beeno')

        self.running = False

    def setUp(self):
        if not self.running:
            self.running = True
            self.startMiniCluster()


    def tearDown(self):
        if self.running:
            self.running = False
            self.shutdownMiniCluster()

    def waitFor(self, tableName, timeout):
        if self.running:
            self.waitTableAvailable(tableName, timeout)
