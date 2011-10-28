#
# Setup jyunit module
#

import difflib
import re
import time
import traceback


import java.lang
from junit.framework import TestCase, AssertionFailedError, ComparisonFailure, TestResult

class SimpleResults(object):
	'''Simple data structure to collect the test result output from running a 
	   given test case.
	'''
	def __init__(self, test):
		self.test = test
		self.startTime = None
		self.endTime = None
		self.failures = []
		self.errors = []
		self.runCount = 0
		self.totalTime = 0
		self.assertCount = 0
		self.testCnt = 0

	def start(self, test):
		# only set this once for a given result obj -- want the first start time
		if self.startTime is None:
			self.startTime = time.time()
		self.runCount = self.runCount + 1

	def end(self, test):
		# always set this, want the last end time
		self.endTime = time.time()
		if self.startTime is not None:
			self.totalTime = self.endTime - self.startTime
		
	def asserted(self, test):
		self.assertCount = self.assertCount + 1
		
	def passed(self):
		return len(self.failures) == 0 and len(self.errors) == 0
	
	def addFailure(self, test, failure):
		self.failures.append(failure)
		
	def addError(self, test, error):
		self.errors.append(error)
		
	def getFailureCount(self):
		return len(self.failures)
	
	def getErrorCount(self):
		return len(self.errors)

	def getTestCount(self):
		return self.runCount

class MultiResults(SimpleResults):
	'''Represents a test class with multiple test cases'''
	def __init__(self, test):
		super(MultiResults, self).__init__(test)
		self.cases = dict()
		
		(cls, name) = getTestInfo(test)
		if cls is not None:
			self.test = cls
			if name is not None and name != cls:
				self.cases[name] = SimpleResults(name)
			
	def start(self, test):
		super(MultiResults, self).start(test)
		res = self.getTestCaseResults(test)
		if res is not None:
			res.start(test)
	
	def end(self, test):
		super(MultiResults, self).end(test)
		res = self.getTestCaseResults(test)
		if res is not None:
			res.end(test)
		
	def addFailure(self, test, failure):
		res = self.getTestCaseResults(test)
		if res is not None:
			res.addFailure(test, failure)
		
	def addError(self, test, error):
		res = self.getTestCaseResults(test)
		if res is not None:
			res.addError(test, error)
		
	def passed(self):
		return all( map(lambda x: x.passed(), self.cases.values()) )
	
	def getFailureCount(self):
		return sum( map(lambda x: x.getFailureCount(), self.cases.values()) )

	def getErrorCount(self):
		return sum( map(lambda x: x.getErrorCount(), self.cases.values()) )
			
	def getTestCount(self):
		return len(self.cases)
	
	def getAllTestCases(self):
		testCases = self.cases.values()
		testCases.sort( lambda x,y: cmp(str(x.test), str(y.test)) )
		return testCases

	def getTestCaseResults(self, test):
		(cls, name) = getTestInfo(test)
		if name != cls:
			if name not in self.cases:
				self.cases[name] = SimpleResults(name)
	
			return self.cases[name]
		
		return None


def getTestInfo(test):
	'''Attempts to extract the test class name and individual test
	name from the given test information'''
	cls = None
	name = None
	'''Returns a tuple of the test class and name'''
	if "getTestClass" in dir(test) and test.getTestClass() is not None:
		if isinstance(test.getTestClass(), java.lang.Class):
			cls = java.lang.Class.getName(test.getTestClass())
			name = cls
		else:
			cls = str(test.getTestClass())
	
	teststr = str(test)
	m = re.match('(\w*(?:\[\d+\])?)\(([\w\.]*)\).*', teststr)
	if m:
		name = m.group(1)
		cls = m.group(2)
	elif "getName" in dir(test):
		name = test.getName()
		
	return (cls, name)
		



# ************ Exception classes ******************************
MAX_TRACE_DEPTH = 5
TEST_FILES = re.compile(".*jyunit/(run|util).py")
TEST_DIR = re.sub("[^/]+$", "", __file__)


class JythonAssertionFailedError(AssertionFailedError):
	'''Override JUnit stack trace handling to provide
	better reporting of error locations in Jython scripts.'''
	def __init__(self, mesg):
		self.traceback = traceback.extract_stack()
		AssertionFailedError.__init__(self, mesg)
	
	def fillInStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		self.setStackTrace( frames )
		return self
	
	def getStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		return frames

	def printStackTrace(self, writer=None):
		if writer is None:
			writer = java.lang.System.err
			
		writer.println("AssertionFailureError: %s" % self.getMessage())
		writer.print("at:")
		# just output the first stack frame, line no and text
		cnt = 0
		for (fname, lineno, func, src) in self.traceback:
			# skip tester.py or test_utils.py lines
			if TEST_FILES.match( fname ):
				continue
			if cnt >= MAX_TRACE_DEPTH:
				break

			fname = re.sub(TEST_DIR, "", fname)
			writer.println("\t%s:%s[%d]\n\t...%s" % ( fname, func, lineno, src ) )
			cnt = cnt + 1
		writer.println()


class JythonComparisonFailure(ComparisonFailure):
	'''Override JUnit stack trace handling to provide
	better reporting of error locations in a Jython context.'''
	
	def __init__(self, mesg, expected, actual):
		self.traceback = traceback.extract_stack()
		self.mesg = mesg
		self.exp = expected
		self.act = actual
		ComparisonFailure.__init__(self, mesg, expected, actual)
	
	def fillInStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		self.setStackTrace( frames )
		return self
	
	def getStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		return frames
	
	def getMessage(self):
		'''Override the standard comparison failure message for strings.  In some large comparisons,
		   the string output is ridiculously long, so we provide a diff version of it.'''
		if self.exp is not None and isinstance( self.exp, str ) \
				and self.act is not None and isinstance( self.act, str ):
			
			expectLines = self.exp.splitlines(1)
			actualLines = self.act.splitlines(1)
			d = difflib.Differ()
			diffs = d.compare(expectLines, actualLines)
			
			if len(expectLines) > 1 or len(actualLines) > 1:
				mesg = ''
				if  self.mesg is not None:
					mesg = self.mesg
				else:
					mesg = 'Actual result did not match expected'
					
				return mesg + ' - diff output:\n' + self.printDiff(diffs)
			
		# default to standard implementation
		return ComparisonFailure.getMessage(self)
	
	def printDiff(self, diffs):
		'''Returns a pseudo-diff formatted string containing the diffs between the
		   expected and actual strings.  A single shared text line is provided before
		   and after each difference for context.  
		   
		   Note: Line numbers are not currently provided in the diff output.'''
		   
		diffout = '--- EXPECTED\n+++ ACTUAL\n'
		lastline = None
		inmatch = False
		
		dcnt = 0
		for l in diffs:
			if len(l) == 0:
				continue
			if l[0] in ('-', '+', '?'):
				if not inmatch:
					dcnt = dcnt + 1
					# include last line for context
					diffout = diffout + ('\nDiff %d:\n%s' % (dcnt, lastline))
				diffout = diffout + l
				inmatch = True
			else:
				if inmatch:
					#first new line after match so output for context
					diffout = diffout + l
					inmatch = False
					
			lastline = l
			
		return diffout
				
		
	def printStackTrace(self, writer=None):
		if writer is None:
			writer = java.lang.System.err
					
		writer.println("ComparisonFailure: %s" % self.getMessage())
		writer.print("at:")
		# just output the first stack frame, line no and text
		cnt = 0
		for (fname, lineno, func, src) in self.traceback:
			# skip tester.py or test_utils.py lines
			if TEST_FILES.match( fname ):
				continue
			if cnt >= MAX_TRACE_DEPTH:
				break

			fname = re.sub(TEST_DIR, "", fname)
			writer.println("\t%s:%s[%d]\n\t...%s" % ( fname, func, lineno, src ) )
			cnt = cnt + 1
		writer.println()

	
class JythonException(java.lang.Exception):
	'''Wraps Python traceback in a Java Exception implementation
	so it can easily be shown in JUnit results.'''
	
	def __init__(self, excType, exc, tb):
		if tb is None:
			tb = traceback.extract_stack()
		self.traceback = tb
		self.exception = exc
		self.excType = excType
		
		excInfo = ""
		if self.exception is not None:
			excInfo = "%s: %s" % ( self.excType.__name__, str(self.exception))
		
		java.lang.Exception.__init__(self, excInfo)

	def fillInStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		self.setStackTrace( frames )
		return self
	
	def getStackTrace(self):
		frames = []
		for (fname, line, func, txt) in self.traceback:
			frames.append( java.lang.StackTraceElement(fname, func, fname, line) )
			
		return frames

	def printStackTrace(self, writer=None):
		if writer is None:
			writer = java.lang.System.err
			
		
			
		writer.println("Jython exception: %s" % self.getMessage())
		writer.print("at:")
		# no length limit and no filtering for errors
		if self.traceback is not None:
			for (fname, lineno, func, src) in self.traceback:
				fname = re.sub(TEST_DIR, "", fname)
				writer.println("\t%s:%s[%d]\n\t...%s" % ( fname, func, lineno, src ) )
			writer.println()

		
