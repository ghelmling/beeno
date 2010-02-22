from org.apache.hadoop.hbase import HBaseClusterTestCase

class HBaseContext(HBaseClusterTestCase):
    def __init__(self):
        super(HBaseContext, self).__init__()
        self.setName('beeno')

        if self.conf.get('test.build.data') is None:
            self.conf.set('test.build.data', '/tmp/beeno')

        self.running = False

    def setUp(self):
        if not self.running:
            self.running = True
            HBaseClusterTestCase.setUp(self)


    def tearDown(self):
        if self.running:
            self.running = False
            HBaseClusterTestCase.tearDown(self)
