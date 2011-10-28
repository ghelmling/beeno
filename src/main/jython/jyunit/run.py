#**********************************************************
#*  Runs all test code under test_src directory.
#*
#**********************************************************

import os
import os.path
import sys
import getopt
import time

from java.io import FileOutputStream, PrintStream, IOException
import java.lang
import java.util
import junit.framework
import junit.runner
import re
import jyunit.util as util
import jyunit.report as report
from jyunit.util import ResultCollector, SimpleResults

# suppress automatic result printing from util
util.__suppress_output__ = True
# disable jyunit exception catching
#sys.excepthook = sys.__excepthook__

# global variable (boo, hiss!)
__debug_enabled = False

# ***********************************************
# Adapters for different types of test cases
#
# Currently supports jython test scripts & JUnit 3 test cases.
# JUnit 4 test supported via JUnit4Adapter class
# ***********************************************

class FileTest(junit.framework.TestCase):
	'''
	This class hooks into the JUnit framework by representing
	each jython test script as an individual test case.
	'''
	def __init__(self, filename, listener=None):
		# translate filename to importable module
		tmpname = re.sub("\.py$", "", filename)
		self.mod_name = tmpname.replace("/", ".")
		self.setName(self.mod_name)
		
		if listener is not None:
			self.listener = listener
	
	def createResult(self):
		util.newResults(self, testlistener=self.listener)
		return util.getResults()
	
	def setUp(self):
		# setup a new test result instance
		#util.newResults(self)
		pass
	
	def tearDown(self):
		# clear the test result instance
		pass
	
	def countTestCases(self):
		# dummy this up -- 1 file == 1 test
		return 1
	
	def modSetUp(self, mod):
		if 'setup' in dir(mod):
			mod.setup()

	def modTearDown(self, mod):
		if 'teardown' in dir(mod):
			mod.teardown()
	
	def run(self, results=None):
		'''
		Runs the test by importing the jython script.  So any top-level
		code is executed.  In addition, if the script contains a function
		named "run_test()", that function is executed.
		'''
		util.startTest(self, results, self.listener)
		debug("Running test: %s" % (self.mod_name)) 
		
		# inject the function handlers before running the code
		testmod = None
		try:
			__import__(self.mod_name, globals(), locals())
			testmod = sys.modules[self.mod_name]
			
			# setup
			try:
				self.modSetUp(testmod)
			except java.lang.Exception, je:
				util.__test_results__.error(je)
			except Exception, e:
				util.__test_results__.error(e)
			
			# check for a run_test module function -- this allows test running
			# to be disabled for normal imports
			if 'run_test' in dir(testmod):
				debug("Executing function: %s.run_test" % self.mod_name)
				testmod.run_test()
				
		except java.lang.Exception, je:
			util.__test_results__.error(je)
		except Exception, e:
			print >> sys.__stdout__, ("Exception %s" % (e))  
			util.__test_results__.error(e)
			
		# teardown
		if testmod is not None:
			try:
				self.modTearDown(testmod)
			except java.lang.Exception, je:
				util.__test_results__.error(je)
			except Exception, e:
				util.__test_results__.error(e)

		util.endTest(self)
		
		# two method versions, one returns results
		if results is None:
			# no arg version
			return util.getResults()
		
	def toString(self):
		'''
		By default JUnit displays "testname(class name)", which is just plain
		ugly here, since the only useful information is the filename.  So just return
		that instead.
		'''
		return self.mod_name
	
	def __str__(self):
		return self.toString()
	

class JUnitTestAdapter(junit.framework.TestCase):
	'''
	Wraps an existing JUnit TestCase instance, so that the test listener can be setup correctly
	'''
	def __init__(self, basetest, listener=None):
		junit.framework.TestCase.__init__(self)
		self.listener = listener		
		self.basetest = basetest
		
	def createResult(self):
		res = self.basetest.createResult()
		if self.listener is not None:
			res.removeListener(self.listener)
			res.addListener(self.listener)
		return res
		#util.newResults(self, testlistener=self.listener)
		#return util.getResults()
		
	def getTestClass(self):
		if "getTestClass" in dir(self.basetest) and self.basetest.getTestClass() is not None:
			return self.basetest.getTestClass()
		
		return None

	def getName(self):
		if "getName" in dir(self.basetest):
			return self.basetest.getName()
		else:
			return str(self.basetest)
	
	def countTestCases(self):
		return self.basetest.countTestCases()
	
	def setUp(self):
		self.basetest.setUp()
	
	def tearDown(self):
		self.basetest.tearDown()
	
	def run(self, results=None):
		'''
		Runs the underlying JUnit TestCase.  First we make sure our
		own test listener is registered with the JUnit results instance
		so we can collect stats for our own reporting.
		'''
		util.startTest(self, results, self.listener)
		debug("Running test: %s" % (self.getName()))
		
		# inject the function handlers before running the code
		try:
			if results is not None:
				results.removeListener(self.listener)
				results.addListener(self.listener)
			else:
				results = self.createResult()
			self.basetest.run(results)
		except java.lang.AssertionError, ae:
			util.__test_results__.fail(ae.getMessage())
		except java.lang.Exception, je:
			util.__test_results__.error(je)
		except Exception, e:
			debug("Exception %s" % (e))
			util.__test_results__.error(e)
			
		util.endTest(self)
		
		# two method versions, one returns results
		if results is None:
			# no arg version
			return util.getResults()

	def toString(self):
		return self.getName()
	
	def __str__(self):
		return self.toString()


class JUnitSuiteAdapter(junit.framework.TestSuite):
	'''
	Our own version of a JUnit TestSuite, which allows us to track
	what java class is actually being tested.
	'''
	def __init__(self, testclass):
		junit.framework.TestSuite.__init__(self, testclass)
		self.testclass = testclass
		
	def getTestClass(self):
		#if __debug_enabled: print >> sys.__stdout__, "returning test class %s " % (str(self.testclass))
		return self.testclass
	
	def countTestCases(self):
		'''
		JUnit annoyingly creates a "warning" test case if there are no actual
		tests to run.  So here we double check and ignore that if we cannot find any
		test* methods in the class.
		'''
		return sum( [1 for x in dir(self.testclass) if x.startswith("test")] )
		


# ***********************************************
# Classes for loading test cases from a directory tree
#
# ***********************************************

#
# Patterns for matching "testable" filenames
#
TEST_FILE_REGEX = re.compile("^test.*\.py$")
JAVA_FILE_REGEX = re.compile("^.*\.java$")
HIDDEN_REGEX = re.compile("^\..*?")
TEST_UTILS_FILE = os.path.basename(util.__file__)


class JyTestLoader(object):
	'''
	Loads jython test scripts from the given directory, matching the naming convention "testxxx.py".
	Each script is run as a separate "test case".
	'''
	def __init__(self, basedir, collector=None, allFiles=False):
		self.basedir = basedir
		self.allFiles = allFiles
		debug("Base dir is %s" % (self.basedir))
		
		# initialize test cases to an empty list
		self.test_cases = []
		
		if collector is not None:
			self.collector = collector
	
	def loadAll(self):
		'''
		Walks the entire test directory tree, and loads
		all files found as test cases. 
		'''
		debug("Loading tests from %s" % self.basedir)
		
		os.path.walk(self.basedir, self.loadEntry, None)

		if __debug_enabled:
			print >> sys.__stdout__, ("Found %d test cases" % len(self.test_cases))
			for t in self.test_cases:
				print >> sys.__stdout__, ("\t%s" % t.getName())
			print >> sys.__stdout__, "" 
		
		
	def loadList(self, filelist):
		'''
		Loads test files from the given list.  A base directory
		must still be provided in the constructor so we can
		resolve the correct relative name.
		'''
		for file in filelist:
			dir, f = os.path.split(file)
			dir = os.path.abspath(dir)
			self.loadFile(dir, f)
		
	def loadEntry(self, arg, dirname, filelist):
		'''
		Attempts to create an individual test case from a script file,
		appending it to the internal list of test cases.
		'''
		#print "Dir %s" % (dirname)
		#print "Files %s" % (str(filelist))
		# skip hidden files and dirs
		if HIDDEN_REGEX.match(dirname):
			return

		# remove hidden files and dirs for dirlist
		for f in filelist:
			if HIDDEN_REGEX.match(f):
				filelist.remove(f)
		
		for f in filelist:
			#print "\t%s" % (f)
			self.loadFile(dirname, f)
			
	def loadFile(self, dirname, f):
		# skip util
		if TEST_UTILS_FILE == f:
			return
		
		if self.isTestFile(dirname, f):
			testcase = self.createTestCase(dirname, f)
			# skip if there are no tests to run
			if testcase is not None:
				if testcase.countTestCases() > 0:
					self.test_cases.append( testcase )
				elif __debug_enabled:
					print >> sys.__stdout__, ("No tests to run, skipping...")
				
	def isTestFile(self, dirname, filename):
		'''
		Determines is a given file constitutes a test.  This allows easy extending 
		of JyTestLoader by implementing this and createTestCase()
		'''
		return (self.allFiles or TEST_FILE_REGEX.match(filename))
	
	def createTestCase(self, dirname, filename):
		'''
		Creates a TestCase instance based on the given file
		'''
		# get the relative dir to the file
		reldir = re.sub("^"+self.basedir+"/?", '', dirname)
		return FileTest( os.path.join(reldir, filename), self.collector )
	
	def getTestSuite(self):
		'''
		Returns a JUnit junit.framework.TestSuite instance containing all our
		internal test cases.
		'''
		suite = junit.framework.TestSuite()
		for t in self.test_cases:
			suite.addTest(t)
			
		return suite


class JavaTestLoader(JyTestLoader):
	'''
	Loads any Java JUnit test cases from the given directory.
	'''
	def __init__(self, basedir, collector=None):
		JyTestLoader.__init__(self, basedir, collector)
	
	def isTestFile(self, dirname, filename):
		debug("Testing dir: %s, file: %s" % (dirname, filename))
		if JAVA_FILE_REGEX.match(filename):
			classFile = self.getClassFile(dirname, filename)
			clazz = self.getClass(classFile)
			if clazz is not None:
				debug("Class file: %s, Class: %s" % (classFile, str(clazz)))
				if junit.framework.Test.isAssignableFrom(clazz) or 'suite' in dir(clazz):
					return True
			
		#print >> sys.__stdout__, ("Not assignable")
		return False
				
	
	def createTestCase(self, dirname, filename):
		classFile = self.getClassFile(dirname, filename)
		clazz = self.getClass(classFile)
		return makeJUnitTest(clazz, self.collector)
	
	
	def getClassFile(self, dirname, filename):
		# get the relative dir to the file
		reldir = re.sub("^"+self.basedir+"/?", '', dirname)
		return os.path.join(reldir, filename)
		
	def getClass(self, fullFilename):
		# remove any file extension
		cname = re.sub('\.java$', '', fullFilename)
		cname = re.sub('/', '.', cname)
		try:
			return java.lang.Class.forName(cname)
		except java.lang.Exception, e:
			debug("Error loading test class: %s" % e.getMessage())
			e.printStackTrace(java.lang.System.err)
			return None
	

class TestRunner(object):
	'''
	Loads test cases and collects the results
	
	NOTE: This is unused for the moment.  Instead we use JUnit to run the tests.
	'''
	def __init__(self, collector=None):
		# extract this module's base dir
		testsmod = util
		
		# initialize test cases to an empty list
		self.test_cases = []
		
		if collector is not None:
			self.collector = collector

	def addSuite(self, testsuite):
		self.addAll( testsuite.tests() )
			
	def addAll(self, tests):
		for t in tests:
			self.test_cases.append(t)
		
	def runAll(self):
		'''
		Convenience function to directly run test cases.  This is currently for debugging
		purposes, as the actual test suites are normally run through the JUnit TestRunner
		instance.
		'''
		for t in self.test_cases:
			# run each test
			t.setUp()
			results = t.run()
			
		self.collector.printAll()
		
	
def debug(mesg):
	'''
	Log debugging information, if enabled
	'''
	if __debug_enabled: print >> sys.__stdout__, (mesg)
	
def makeJUnitTest(clazz, collector):
	junittest = None
	if clazz is not None:
		if junit.framework.Test.isAssignableFrom(clazz):
			junittest = JUnitSuiteAdapter(clazz)
		elif 'suite' in dir(clazz):
			junittest = clazz.suite()

	if junittest is not None:
		return JUnitTestAdapter( junittest, collector )
	
	return None
	
	
# ***********************************************
# Main execution
# 
# ***********************************************

def getOptions():
	# get any passed in options
	try:
		optlist, args = getopt.getopt( sys.argv[1:], "hvo:r:d:p:j:t:x:f:", \
			["help", "verbose", "output=", "recipients=", "directory=", "product=", "javasrc=", "testclass=", "xmldir=", "file=", "debug"] )
	except getopt.GetoptError:
		usage()
		sys.exit(2)
		
	# store options in a simple object
	opts = dict()
	opts['verbose'] = False
	opts['product'] = ''
	for o, v in optlist:
		if o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-v", "--verbose"):
			opts['verbose'] = True
		elif o in ("-o", "--output"):
			opts['output'] = v
		elif o in ("-r", "--recipients"):
			opts['recipients'] = v
		elif o in ("-d", "--directory"):
			opts['directory'] = v
		elif o in ("-p", "--product"):
			opts['product'] = v
		elif o in ("-j", "--javasrc"):
			opts['javasrc'] = v
		elif o in ("-t", "--testclass"):
			opts['testclass'] = v
		elif o in ("-x", "--xmldir"):
			opts['xmldir'] = v
		elif o in ("-f", "--file"):
			opts['inputfile'] = v
		elif o == "--debug":
			global __debug_enabled
			__debug_enabled = True
			util.__debug_enabled = True
			
	return (opts, args)

def usage():
	print ''' 
Usage:\t%s [options] [testfile1 testfile2 ... ]

  -h, --help               Show this usage information
  -o, --output=filename    Write test results to this file
  -r, --recipients=emails  Email the test results to these recipients (comma separated)
  -d, --directory=path     Use this as the test src directory
  -j, --javasrc=path       Source directory for Java JUnit test cases
  -t, --testclass=clsname  To load and run an individual JUnit test case
  -f, --file=filename      Read list of test files from filename
  -p, --product=name       Product name ("meetup", "alliance"), only used for output
  -x, --xmldir=dirname     Output XML test results to directory (format same as Ant JUnit task XML formatter)
  -v, --verbose            Show extra information as tests are running
  --debug                  Show debug messages for test running''' % (sys.argv[0])
	
	
def createOutput( outfile ):
	try:
		fout = FileOutputStream( outfile )
		return PrintStream( fout )
	except IOException:
		print "Error opening output file"
		return None
	
	
def main(opts, args):
	'''
	Main execution when the run script is called directly
	'''
	runResults = ResultCollector()
	alltests = junit.framework.TestSuite()
	
	if 'testclass' in opts:
		# single test class
		try:
			clazz = java.lang.Class.forName(opts['testclass'])
			alltests.addTest(makeJUnitTest(clazz, runResults))
		except java.lang.Exception, e:
			print >> sys.__stdout__, ("Error loading test class: %s" % e.getMessage())
			e.printStackTrace(java.lang.System.err)
			sys.exit(2)
	else:
		# read in list of tests from input file, if specified
		filelist = None
		if 'inputfile' in opts:
			fin = None
			filelist = []
			try:
				try:
					if opts['inputfile'] == '-':
						# using stdin
						fin = sys.stdin
					else:
						fin = open(opts['inputfile'])
						
					filelist = [l.strip() for l in fin]
				finally:
					if fin is not None: fin.close()
			except IOError, ioe:
				print >> sys.__stdout__, ("Error reading input file %s" % opts['inputfile'])
				print >> sys.__stderr__, str(ioe)
				sys.exit(3)
				
			debug("Input file list is:\n%s\n" % ('\n'.join(filelist)))

		# add the specified directory to the python path
		if 'directory' in opts:
			d = os.path.abspath( opts['directory'] )
			if d not in sys.path:
				sys.path.append(d)
				#print "PYTHONPATH is %s" % (sys.path)
				
			loader = JyTestLoader(d, runResults)
			# if files listed, use those
			if filelist is not None:
				# override test filename matching -- since the files are specified
				loader.allFiles = True
				pyfiles = [f for f in filelist if f.endswith(".py")]
				loader.loadList(pyfiles)
			else:
				# otherwise walk the directory
				loader.loadAll()

			for x in loader.getTestSuite().tests():
				alltests.addTest(x)
		
		# load any Java JUnit tests
		javaloader = None
		if opts.has_key('javasrc'):
			javaloader = JavaTestLoader(opts['javasrc'], runResults)
			# use explicit file list, if specified
			if filelist is not None:
				javafiles = [f for f in filelist if f.endswith(".java")]
				javaloader.loadList(javafiles)
			else:
				javaloader.loadAll()

			if __debug_enabled: print >> sys.__stdout__, ("Found %d java tests" % (len(javaloader.test_cases)))
			
			for x in javaloader.getTestSuite().tests():
				alltests.addTest(x)
	
	# direct output to file if specified
	out = java.lang.System.err
	if opts.has_key('output'):
		out = createOutput( opts['output'] )
		
	# redirect stdout to temp file for test modules
	testout = None
	if not opts['verbose']:
		testout = open( time.strftime('/tmp/tester_stdout.log'), 'w')
		sys.stdout = testout


	# run with JUnit
	runner = None
	runner = junit.textui.TestRunner()
	
	runner.doRun( alltests )
	
	printer = report.TextPrinter(runResults, out)
	printer.printAll()
	print >> sys.__stdout__, ("ERROR COUNT: %d" % runResults.totalErrors)
	
	# output XML results if requested
	if 'xmldir' in opts:
		try:
			dirname = os.path.abspath(opts['xmldir'])
			xmlout = report.XMLPrinter(runResults, dirname)
			xmlout.printAll()
		except IOError, ioe:
			print >> sys.__stderr__, ("Unable to output XML results: %s" % str(ioe))
	
	if testout is not None:
		testout.flush()
		testout.close()
		
	# send out an email notice
	if opts.has_key("recipients") and opts.has_key("output"):
		report.sendReport( runResults, opts['output'], opts['recipients'], opts['product'] )
	
	
if __name__ == '__main__':
	# first load any options
	(opts, args) = getOptions()
		
	# check for at least one of the required sources
	if not ('directory' in opts or 'javasrc' in opts or 'testclass' in opts) and len(args) == 0:
		usage()
		sys.exit(2)
		
	main(opts, args)
