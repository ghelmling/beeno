from java.util import HashMap
from java.util import ArrayList
from junit.framework import TestCase, AssertionFailedError, ComparisonFailure, TestResult
import junit
from types import MethodType, FunctionType
import traceback
import java.lang
import re
import sys
import time
import difflib

from jyunit import SimpleResults, MultiResults, JythonAssertionFailedError, JythonComparisonFailure, JythonException, getTestInfo
from jyunit.report import TextPrinter

exportedNames = ['makemap', 'FunctionHandler', 'DummyTest', 'AssertHandler', 'printTestResults']


def makemap(**vals):
    map = HashMap()
    for k,v in vals.items():
        print "setting %s : %s" % ( k,v ) 
        map[k] = v
    return map

def makelist(*vals):
    list = ArrayList()
    for v in vals:
        list.add(v)
    return list


class FunctionHandler(object):
	def __init__(self, test_case, func):
		self.test_case = test_case
		self.func = func
		
	def __call__(self, *args):
		#print "Calling %s" % (self.func)
		apply(self.func, (self.test_case,) + args)

# ******************* JUnit integration/emulation *********************
	
class SimpleTest(TestCase):
	'''Dummy TestCase implementation to provide easy integration
	into JUnit reporting and running.'''
	def __init__(self, name=None):
		if name is None:
			name = __name__
		self.setName(name)
		
	def createResult(self):
		test_utils.newResults(self)
		return test_utils.getResults()
		
	def run(self):
		'''There is nothing to run here.  The tests conditions
		are actually invoked in the opposite direction using the 
		assert*() and fail*() functions.  This class just fills
		in compatibility with the Junit framework.'''
		pass
		
		
	def toString(self):
		'''By default JUnit displays "testname(class name)", which is just plain
		ugly here, since the only useful information is the filename.  So just return
		that instead.'''
		return self.name
	
	def __str__(self):
		return self.toString()
		
# *************** Assertion handler *****************************

class AssertHandler(object):
	'''Simply implementation of JUnit Assert methods
	to allow these to be called directly by importing 
	code.'''
	
	def __init__(self, test_case, results=None, listener=None):
		self.test_case = test_case
		if results is None:
			results = TestResult()
		self.test_results = results
		
		self.listener = None
		self.setListener( listener )
			
	def setListener(self, listener):
		if listener is not None:
			self.listener = listener
			# try clearing the listener if already registered
			self.test_results.removeListener(listener)
			self.test_results.addListener(listener)
		
	def assertEquals(self, actual, expected, mesg=None, delta=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if delta is not None:
			# check if these are both numbers and we have an acceptable buffer window
			# for floating point inaccuracies
			if abs(actual - expected) > delta:
				if mesg is None:
					mesg = "Values are not equal (within allowed delta)"
				self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(expected), str(actual)))		
			
		elif expected != actual:
			if mesg is None:
				mesg = "Values are not equal"
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(expected), str(actual)))
						
	def assertNotEquals(self, actual, expected, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if expected == actual:
			if mesg is None:
				mesg = "Values are equal, but expected not"
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(expected), str(actual)))

	def assertMoreThan(self, actual, expected, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if not actual > expected:
			if mesg is None:
				mesg = "Value is less than expected "
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(expected), str(actual)))

	def assertLessThan(self, actual, expected, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if not actual < expected :
			if mesg is None:
				mesg = "Value is more than expected "
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(expected), str(actual)))

	def assertFalse(self, condition, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if (condition):
			if mesg is None:
				mesg = "Expected false value, but condition was true"
			self.test_results.addFailure(self.test_case, JythonAssertionFailedError(mesg))

	def assertMatches(self, actual, pattern, mesg=None, flags=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		matchval = None
		if flags is not None:
			matchval = re.match(pattern, actual, flags)
		else:
			matchval = re.match(pattern, actual)
			 
		if not matchval:
			if mesg is None:
				mesg = "Value doesn't match regex "
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(pattern), str(actual)))

	def assertNotMatches(self, actual, pattern, mesg=None, flags=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		matchval = None
		if flags is not None:
			matchval = re.match(pattern, actual, flags)
		else:
			matchval = re.match(pattern, actual)
			 
		if matchval:
			if mesg is None:
				mesg = "Value matches regex, but shouldn't "
			self.test_results.addFailure(self.test_case, JythonComparisonFailure(mesg, str(pattern), str(actual)))

		
	def assertNotNull(self, obj, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if (obj is None):
			if mesg is None:
				mesg = "Object was null, but expected non-null" 
			self.test_results.addFailure(self.test_case, JythonAssertionFailedError(mesg))
		
	def assertNull(self, obj, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if (obj is not None):
			if mesg is None:
				mesg = "Object was not null, but expected null reference" 
			self.test_results.addFailure(self.test_case, JythonAssertionFailedError(mesg))
		
	def assertNotSame(self, actual, expected, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if (expected == actual):
			if mesg is None:
				"Expected references to different objects" 
			self.test_results.addFailure(self.test_case, JythonComparisonFailure("Object references should be different instances", str(expected), str(actual)))
						
	def assertTrue(self, condition, mesg=None):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if not condition:
			if mesg is None:
				mesg = "Expected condition to be true, but was false"
			self.test_results.addFailure(self.test_case, JythonAssertionFailedError(mesg))

	def assertExcept(self, func, targ, exc, *args):
		if self.listener is not None:
			self.listener.assertCalled( self.test_case )

		if not callable(func):
			self.fail("Function %s not callable" % str(func))
			return
		
		callargs = [targ]
		if args is not None and len(args) > 0:
			callargs[1:] = args;
		
		try:
			apply(func, callargs)
			self.fail("Expected %s calling %s" % (str(exc), str(func)))
		except exc, e:
			# expected condition
			pass
		except Exception, e:
			self.fail("Unexpected exception %s calling %s" % (str(e), str(func)))


			
	def fail(self, mesg=None):
		if mesg is None:
			mesg = "Test failed"
		self.test_results.addFailure(self.test_case, JythonAssertionFailedError(mesg))
		
	def error(self, err):
		if isinstance(err, java.lang.Throwable):
			self.test_results.addError(self.test_case, err)
		elif isinstance(err, Exception):
			#traceback.print_exc()
			if sys.exc_traceback is not None:
				tb = traceback.extract_tb( sys.exc_traceback )
			else:
				tb = traceback.extract_stack()
			self.test_results.addError(self.test_case, JythonException(sys.exc_type, err, tb))
		else:
			self.fail('Error: %s' % (str(err)))
		
	def getResults(self):
		return self.test_results
	
	def printResults(self, cnt=None):
		if cnt is not None:
			print "%d)" % (cnt)
		if (self.test_results.failureCount() + self.test_results.errorCount()) == 0:
			print "Test %s: SUCCESS!" % (self.test_case.getName())
		else:
			print "Test %s: FAILED!\n" % self.test_case.getName()
			
			# show test failures
			print "%d failures" % self.test_results.failureCount()
			failEnum = self.test_results.failures()
			cnt = 1
			while (failEnum.hasMoreElements()):
				nextFail = failEnum.nextElement()
				print "Failure %d: %s" % (cnt, nextFail.exceptionMessage())
				print nextFail.trace()
				cnt = cnt + 1

			print ""
			
			# show test errors
			print "%d errors" % self.test_results.errorCount()
			failEnum = self.test_results.errors()
			cnt = 1
			while (failEnum.hasMoreElements()):
				nextFail = failEnum.nextElement()
				print "Error %d:\t%s" % (cnt, nextFail.exceptionMessage())
				print nextFail.trace()
				cnt = cnt + 1

			print ""
			print "Test summary: %d failure, %d errors" % (self.test_results.failureCount(), self.test_results.errorCount())
		

def exportAssertions(handler):
	# Expose the assertion method for importing code
	for f in handler.__class__.__dict__.keys():
		fref = handler.__class__.__dict__[f]
		#print "Checking %s, type %s" % (f, type(fref))
		if (type(fref) is MethodType or type(fref) is FunctionType) and (f.startswith("assert") or f.startswith("fail")):
			#print "Setting %s" % f
			globals()[f] = FunctionHandler(handler, handler.__class__.__dict__[f])
			exportedNames.append(f)
				
# print out the test results
def printTestResults(onexit=False):
	global __test_results__
	global __suppress_output__
	# do nothing for auto call when suppressed
	if onexit and __suppress_output__:
		return 
	
	# print out all queued results
	TextPrinter(__completed_results__).printAll()


def failOnException(exc_type, exc_value, exc_trace):
	global __test_results__
	global __debug__

	# always bail in debug mode
	if __debug_enabled:
		sys.__excepthook__(exc_type, exc_value, exc_trace)
	elif isinstance(exc_value, java.lang.Error) and not isinstance(exc_value, java.lang.AssertionError):
		# bail on java errors
		sys.__excepthook__(exc_type, exc_value, exc_trace)	
	elif isinstance(exc_value, Exception):
		sys.__excepthook__(exc_type, exc_value, exc_trace)
		
	__test_results__.error(exc_value)

def newResults(testcase, testresults=None, testlistener=None):
	global __test_results__
	global __completed_results__
	if testlistener is None:
		testlistener = __completed_results__
		
	__test_results__ = AssertHandler(testcase, testresults, testlistener)

	# add the current results to the completed queue
	#__completed_results__.append(__test_results__)
	exportAssertions(__test_results__)
	
def startTest(testcase, testresults=None, testlistener=None):
	#newResults(testcase, testresults, testlistener)
	global __test_results__
	__test_results__.test_case = testcase
	__test_results__.setListener( testlistener )
	getResults().startTest(testcase)
	
def endTest(testcase):
	getResults().endTest(testcase)
	
def getResults():
	global __test_results__;
	return __test_results__.getResults()


def runTest(testcase):
	# initialize new results
	#newResults(testcase)
	startTest(testcase)
	try:
		testcase.run()
	except java.lang.Exception, je:
		__test_results__.error(je)
	except Exception, e:
		__test_results__.error(e)
	endTest(testcase)

	

class ResultCollector(junit.framework.TestListener):
	'''Stores test results indexed by tests, so we can easily retrieve stats.
	   This is used the by jyunit.run module to collect test results over a batch test run,
	   so that results can be printed at the completion of the test run.
	   
	   Internally, a SimpleResults instance is stored for each test case.
	'''
	def __init__(self):
		self.results = dict()
		self.totalErrors = 0
		# overall test run time
		self.runStart = None
		self.runStop = None
		
		# collect some basic info on the run environment
		import socket
		self.host = socket.gethostname()
	

	def startTest(self, test):
		if self.runStart is None:
			self.runStart = time.time()
		self.getResults(test).start(test)

	def endTest(self, test):
		self.runStop = time.time()
		self.getResults(test).end(test)
			
	def addFailure(self, test, failedError):
		r = self.getResults(test)
		r.addFailure(test, junit.framework.TestFailure(test, failedError) )
		self.totalErrors = self.totalErrors + 1
	
	def addError(self, test, exception):
		# treat assertions as failures, not errors
		if isinstance(exception, java.lang.AssertionError):
			self.addFailure(test, exception)
			return

		r = self.getResults(test)
		r.addError(test, junit.framework.TestFailure(test, exception) )
		self.totalErrors = self.totalErrors + 1
		
	def assertCalled(self, test):
		self.getResults(test).asserted(test)
			
	def getTestName(self, test):
		(cls, name) = getTestInfo(test)
		return name
		
	def getResults(self, test):
		(tcls, tname) = getTestInfo(test)
		key = tcls
		if tcls is None:
			key = tname
		res = None
		if not self.results.has_key(key):
			if tcls is not None:
				self.results[key] = MultiResults(test)
			else:
				self.results[key] = SimpleResults(test)

		return self.results[key]
	
	def splitResults(self):
		passed = []
		failed = []
		
		for (k, v) in self.results.items():
			if v.passed():
				passed.append(v)
			else:
				failed.append(v)
				
		if len(passed) > 0:
			passed.sort( lambda x,y: cmp(str(x.test), str(y.test)) )
		if len(failed) > 0:
			failed.sort( lambda x,y: cmp(str(x.test), str(y.test)) )
		return (passed, failed)
	
	def runPassed(self):
		(p, f) = self.splitResults()
		return len(f) == 0
			
	def totalRunCount(self):
		return len(self.results)
	
	def totalRunTime(self):
		if self.runStop is not None and self.runStart is not None:
			return self.runStop - self.runStart
		
		return 0
		

		
# ******************************************************************
# Setup for reporting on exit when directly running a test script
# ******************************************************************

# store completed tests in a list until program exit
__completed_results__ = ResultCollector()

# provide access to junit asset* methods for direct execution of files
newResults(SimpleTest(sys.argv[0]), testlistener=__completed_results__)

__suppress_output__ = False
__debug_enabled = False

# Add a global exception handling hook to report uncaught exceptions as errors
sys.excepthook = failOnException

# Register an exit function to print out the results when a script is run directly
import atexit
atexit.register(printTestResults, True)
