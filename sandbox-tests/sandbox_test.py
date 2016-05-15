import happybase
import subprocess
import os

"""
Functional Tests for Sandbox features
Usage: Run the tests using the below command
nosetests sandbox_test.py
"""

# initialize
conn = happybase.Connection('localhost')
scripts_dir = '/mapr/my.cluster.com/software/philips-sandbox'
production_table_path = '/dataset/production'
sandbox_table_path = '/dataset/sandbox_test01'
production_table = conn.table('/dataset/production')
sandbox_table = conn.table('/dataset/sandbox_test01')
cluster_path = '/mapr/my.cluster.com'

# Test Class
# prefix all test methods with "test_", so that it can be recognized to be run by nosetests
class Sandbox_Test:

    def create_new_sandbox(self):
        try:
            subprocess.call('%s/sandbox-admin-api/bin/sandboxcli.sh create -original %s -path %s' % (scripts_dir, production_table_path, sandbox_table_path), shell=True)
        except:
            print 'ERROR creating table'

    def test_create_new_sandbox(self):
        print "test creation of new sandbox table"
        self.create_new_sandbox()
        assert sandbox_table.families().keys() == ['_shadow'] + production_table.families().keys()

    def delete_sandbox(self):
        subprocess.call('%s/sandbox-scripts/sandbox_delete.sh %s' % (scripts_dir, sandbox_table_path), shell=True)

    def test_delete_sandbox(self):
        self.delete_sandbox()
        print "test deletion of new sandbox table"
        assert os.path.exists('%s/dataset/sandbox01_meta' % (cluster_path)) == True
