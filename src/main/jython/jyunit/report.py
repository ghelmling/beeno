#**********************************************************
#*  Classes and utilities for outputting test results
#*
#**********************************************************

import os.path
import re
import smtplib
import time

import java.lang
import java.util
import java.io

from jyunit import SimpleResults, MultiResults, JythonAssertionFailedError, JythonComparisonFailure, JythonException

# regex for class names to skip as "internal" in stack traces
INTERNAL_CLASS_REGEX = re.compile("(org\.python\.|sun\.reflect\.|java\.lang\.reflect\.|org\.junit\.|junit\.)")

class ResultPrinter(object):
	'''Base class for outputting test results'''
	def __init__(self, results):
		self.results = results

	def getFailureStack(self, exception):
		'''Returns the contents of the exception stack trace as a string'''
		strout = java.io.StringWriter()
		pout = java.io.PrintWriter(strout)
		outtxt = ''
		try:
			exception.printStackTrace(pout)
			pout.flush()
			outtxt = strout.toString()
		finally:
			pout.close()
			strout.close()
			
		return outtxt
		

class TextPrinter(ResultPrinter):
	'''Prints test results in a plain text format suitable for simple email reports.  The output
	   consists of 4 separate sections:
	    * a header containing overall stats (total # of tests, errors and failures)
	    * summary list of individual test class failures
	    * summary list of passed test classes
	    * detailed failure and error output by test class
	    
	'''
	
	def __init__(self, results, out=java.lang.System.err):
		self.results = results
		self.out = out
		
	def printAll(self):
		(passed, failed) = self.results.splitResults()
		self.printHeader(self.out, passed, failed)
		self.printSummary(self.out, passed, failed)
		# print failure details
		if len(failed) > 0:
			self.out.println("DETAILS: ")
			self.out.println("-"*50)
			self.printFailedDetails(self.out, failed)

	def printHeader(self, out, passed, failed):
		out.print( \
'''\
Run: %d,  Passed: %d,  Failed: %d
Generated %s
Run Time: %d sec

''' % ( self.results.totalRunCount(), len(passed), len(failed), java.util.Date().toString(), self.results.totalRunTime() ) )

	def printSummary(self, out, passed, failed):
		if len(failed) > 0:
			out.println("FAILED: ")
			out.println("-"*50)
			self.printFailed(out, failed)
			out.println()

		if len(passed) > 0:
			out.println("SUCCESSFUL:  ")
			out.println("-"*50)
			self.printPassed(out, passed)
			out.println()		
		

	def getTestCountText(self, res):
		extrainfo = ""
		if isinstance(res, MultiResults):
			extrainfo = "%3d tests     "  % (res.getTestCount())
		if res.assertCount > 0:
			if extrainfo:
				extrainfo = extrainfo + "; "
			extrainfo = extrainfo + ("%3d assertions" % (res.assertCount))
		
		return extrainfo
	
	def printPassed(self, out, passed):
		for res in passed:
			out.println( "%-60s    %4d sec; %s" % ('[ '+str(res.test)+' ]:', res.totalTime, self.getTestCountText(res)) )

	def printFailed(self, out, failed):
		
		fcnt = 1
		for f in failed:
			out.println( "%-60s    %4d sec;  %s  %3d failures  %3d errors" \
						% ( '[ '+str(f.test)+' ]:', f.totalTime, self.getTestCountText(f), f.getFailureCount(), f.getErrorCount() ) )

	def printFailedDetails(self, out, failed):
		for f in failed:
			self.printFailure(out, f)
			
	def printFailure(self, out, f, depth=0):
		if depth == 0:
			out.print( \
'''%-40s 	  %4d sec;    %s  %3d failures  %3d errors
''' % ( '[ '+str(f.test)+' ]:', f.totalTime, self.getTestCountText(f), f.getFailureCount(), f.getErrorCount() ) )
		else:
			out.println( "( %s ) " % (str(f.test)) )

		if isinstance(f, MultiResults):
			for c in f.getAllTestCases():
				if c.getErrorCount() > 0 or c.getFailureCount() > 0:
					self.printFailure(out, c, depth+1)
		else:
			if f.getFailureCount() > 0:
				out.println('    Failures:')
				self.printErrors(out, f.failures, True)
			if f.getErrorCount() > 0:
				out.println('    Errors:')
				self.printErrors(out, f.errors, False)
		out.println()

	def printErrors(self, out, errors, abbrev=False):
		indent = ' '*4
		subindent = ' '*8
		if len(errors) > 0:
			failCnt = 1
			for e in errors:
				out.print( '''%s%d. ''' % (indent, failCnt) )
				if abbrev:
					self.printFailureStack(out, e.thrownException(), subindent)
				else:
					e.thrownException().printStackTrace(out)
				failCnt = failCnt + 1

	def printFailureStack(self, out, exception, indent):
		'''Prints an abbreviated version of the stack trace for failures'''
		# delegate to custom exceptions
		if isinstance(exception, JythonAssertionFailedError) or \
		   isinstance(exception, JythonComparisonFailure) or \
		   isinstance(exception, JythonException):
			exception.printStackTrace(out)
			return
		
		out.println("%s: %s" % (exception.getClass().getName(), exception.getMessage()))
		skipping = False
		for elm in exception.getStackTrace():
			if INTERNAL_CLASS_REGEX.match(elm.getClassName()):
				if not skipping:
					skipping = True
					out.println("%s    ..." % (indent))
				continue
			
			skipping = False
			out.println("%s at %s" % (indent, elm.toString()))
		
		# TODO: chain exception output

class XMLPrinter(ResultPrinter):
	'''Outputs test results in an XML format like that used by the Ant JUnit task.
	   The results for each test class are output as a separate XML file in the common
	   directory passed to the constructor.  Each file is named in the format:
	   TEST-<classname>.xml
	'''
	
	def __init__(self, results, dirname):
		self.results = results
		self.dirname = dirname
		from xml.dom.minidom import getDOMImplementation
		self.dom = getDOMImplementation()
		
	def printAll(self):
		'''For each test class in the results, outputs an XML file of results for that class'''
		for t in self.results.results.values():
			self.printTest(t)
	
	def printTest(self, test):
		doc = self.dom.createDocument(None, "testsuite", None)
		root = doc.documentElement
		root.setAttribute("errors", str(test.getErrorCount()))
		root.setAttribute("failures", str(test.getFailureCount()))
		root.setAttribute("hostname", self.results.host)
		root.setAttribute("name", str(test.test))
		root.setAttribute("tests", str(test.getTestCount()))
		root.setAttribute("time", str(test.totalTime))
		root.setAttribute("timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(test.startTime)))
		# add runtime properties
		sysprops = java.lang.System.getProperties()
		propselt = doc.createElement("properties")
		for n in sysprops.propertyNames():
			prop = doc.createElement("property")
			prop.setAttribute("name", n)
			prop.setAttribute("value", sysprops.getProperty(n))
			propselt.appendChild(prop)
			
		root.appendChild(propselt)
		
		# add individual test cases
		if isinstance(test, MultiResults):
			for c in test.getAllTestCases():
				self.printTestCase(doc, test, c)
		else:
			# print as a single test case
			self.printTestCase(doc, test, test, "[main]")
			# add any top level errors
			#if test.getErrorCount() > 0:
			#	for e in test.errors:
			#		self.printError("error", doc, root, e)
			#if test.getFailureCount() > 0:
			#	for f in test.failures:
			#		self.printError("failure", doc, root, f)
  
		# TODO: actually capture output here
		root.appendChild( doc.createElement("system-out") )
		root.appendChild( doc.createElement("system-err") )

		# output the document
		fname = "TEST-%s.xml" % (test.test)
		outfile = os.path.join(self.dirname, fname)
		fout = open(outfile, "w")
		try:
			doc.writexml(fout, addindent="  ", newl="\n", encoding="UTF-8")
			doc.unlink()
		finally:
			fout.close()
			
	def printTestCase(self, doc, test, caseresults, casename=None):
		if casename is None:
			casename = str(caseresults.test)
		root = doc.documentElement
		caseelt = doc.createElement("testcase")
		caseelt.setAttribute("classname", str(test.test))
		caseelt.setAttribute("name", casename)
		caseelt.setAttribute("time", str(caseresults.totalTime))
		root.appendChild(caseelt)

		# output any errors
		if caseresults.getErrorCount() > 0:
			for e in caseresults.errors:
				self.printError("error", doc, caseelt, e)
		if caseresults.getFailureCount() > 0:
			for f in caseresults.failures:
				self.printError("failure", doc, caseelt, f)
				
	def printError(self, eltType, doc, parent, err):
		elt = doc.createElement(eltType)
		if err.thrownException(): 
			if err.thrownException().getMessage():
				elt.setAttribute("message", err.thrownException().getMessage())

			elt.setAttribute("type", err.thrownException().getClass().getName())
			elt.appendChild( doc.createTextNode(self.getFailureStack(err.thrownException())) )
			
		parent.appendChild(elt)


def sendReport( collector, fname, recips, product='' ):
	'''Emails the test report output to the comma-separated email addresses in "recips"'''
	f = open(fname)
	reportText = f.read()
	f.close()
	reciplist = recips.split(",")
	if product is not None and len(product) > 0:
		product = ' ' + product
		
	errorcnt = ''
	errortxt = 'error'
	if collector.totalErrors > 0:
		if collector.totalErrors > 1:
			errortxt = 'errors'
		errorcnt = "%d %s" % (collector.totalErrors, errortxt)
	else:
		errorcnt = "Success"
		
	mesg = \
'''\
From: %s
To: %s
Subject: {DEV MODE} [%s] %s Test Run: %s

%s
''' % ('nobody@meetup.com', ', '.join(reciplist), collector.host, product, errorcnt, reportText)
	
	server = smtplib.SMTP('mail.int.meetup.com')
	server.sendmail('nobody@meetup.com', reciplist, mesg)
	server.quit()
		
					
