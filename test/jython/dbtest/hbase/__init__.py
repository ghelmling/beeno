from org.apache.hadoop.hbase import HBaseTestingUtility

class HBaseContext(HBaseTestingUtility):
    def __init__(self):
        super(HBaseContext, self).__init__()
        self.setName('beeno')

        if self.conf.get('test.build.data') is None:
            self.conf.set('test.build.data', '/tmp/beeno')

        self.running = False

    def setUp(self):
        if not self.running:
            self.running = True
            HBaseTestingUtility.startMiniCluster(self)


    def tearDown(self):
        if self.running:
            self.running = False
            HBaseTestingUtility.shutdownMiniCluster(self)
