#
# Utilities for interacting with HBase 
#

from org.apache.hadoop.hbase import HBaseConfiguration, HColumnDescriptor, HTableDescriptor, HConstants, KeyValue
from org.apache.hadoop.hbase.client import HBaseAdmin, HTable, Get, Put, Delete, Scan
from org.apache.hadoop.hbase.filter import FilterList
from org.apache.hadoop.hbase.util import Bytes

import java.lang
import java.util

import com.google.protobuf

from meetup.beeno.filter import ColumnMatchFilter
from meetup.beeno.util import PBUtil

# constants for dicts
## column family options
BLOCKCACHE = "blockcache"
BLOOMFILTER = "bloomfilter"
COMPRESSION = "compression"
MEMORY = "inmemory"
INDEXINTERVAL = "indexinterval"
LENGTH = "maxlength"
VERSIONS = "maxversions"
TTL = "ttl"

## table options
FILESIZE = "filesize"
MEMCACHEFLUSH = "memcacheflush"
READONLY = "readonly"

class Admin(object):
	'''
	Utility class for administering HBase tables.  This class basically just provides
	more convenient access to the commonly used org.apache.hadoop.hbase.client.HBaseAdmin
	methods.
	'''
	def __init__(self, conf=None):
		if conf is None:
			conf = HBaseConfiguration()
		
		self.hadmin = HBaseAdmin(conf)
		
	def create(self, tablename, cols, tableopts=None):
		'''
		Creates a new HBase table with the given name, column families, indexes and options.
		'''
		tdef = HTableDescriptor(tablename)
		# set any table options
		if tableopts is not None:
			if MEMORY in tableopts:
				tdef.setInMemory( bool(tableopt[MEMORY]) )
			if FILESIZE in tableopts:
				tdef.setMaxFileSize( int(tableopt[FILESIZE]) )
			if MEMCACHEFLUSH in tableopts:
				tdef.setMemcacheFlushSize( int(tableopt[MEMCACHEFLUSH]) )
			if READONLY in tableopts:
				tdef.setReadOnly( bool(tableopt[READONLY]) )
				
		# setup column families and options
		for k, v in cols.items():
			colfamily = HColumnDescriptor(str(k))
			if BLOCKCACHE in v:
				colfamily.setBlockCacheEnabled(bool(v[BLOCKCACHE]))
			if BLOOMFILTER in v:
				colfamily.setBloomfilter(bool(v[BLOOMFILTER]))
			if COMPRESSION in v:
				colfamily.setCompressionType(v[COMPRESSION])
			if MEMORY in v:
				colfamily.setInMemory(bool(v[MEMORY]))
			if INDEXINTERVAL in v:
				colfamily.setMapFileIndexInterval(int(v[INDEXINTERVAL]))
			if LENGTH in v:
				colfamily.setMaxValueLength(int(v[LENGTH]))
			if VERSIONS in v:
				colfamily.setMaxVersions(int(v[VERSIONS]))
			if TTL in v:
				colfamily.setTimeToLive(int(v[TTL]))
				
			tdef.addFamily(colfamily)
		
		self.hadmin.createTable(tdef)
		
	def drop(self, tablename):
		'''Convenience method to first disable and then delete a given table.'''
		self.disable(tablename)
		self.delete(tablename)
		
	def describe(self, tablename):
		'''
		Returns the full org.apache.hadoop.hbase.HTableDescriptor instance
		for the given table
		'''
		return self.hadmin.getTableDescriptor(tablename)
	
	def exists(self, tablename):
		'''Checks if the given table exists'''
		return self.hadmin.tableExists(tablename)
	
	def show(self):
		'''
		List the names (only) all currently defined tables.
		For full table definitions, see Admin.describeAll()
		'''
		return [ x.getNameAsString() for x in self.describeAll() ]

	def describeAll(self):
		'''
		Returns a list of org.apache.hadoop.hbase.HTableDescriptor
		instances for all tables.
		'''
		return self.hadmin.listTables()
	
	def enable(self, tablename):
		'''Brings a previously disabled table back online'''
		self.hadmin.enableTable(tablename)
		
	def disable(self, tablename):
		'''Takes a given table offline in HBase'''
		self.hadmin.disableTable(tablename)

	def delete(self, tablename):
		'''
		Completely removes the given table.  This can only be called on tables
		that have already been disabled.  To disable and delete in one step,
		see Admin.drop(tablename)
		'''
		self.hadmin.deleteTable(tablename)


# dict key for row key
ROW_KEY = '__key__'

class Table(object):
	def __init__(self, tablename, conf=None):
		if conf is None:
			conf = HBaseConfiguration()

		self.table = HTable(conf, tablename)

	def get(self, rowkey):
		'''
		Retrieves the specific table row as a dictionary, with
		full column names (column family:name) as keys, and deserialized
		(from protocol buffers) values as values.

		If the row does not exist, returns None.
		'''
		op = Get( Bytes.toBytes( java_str(rowkey) ) )
		row = self.table.get(op)
		if row is not None and not row.isEmpty():
			return todict(row)

		return None

	def getVersions(self, rowkey, limit=None, ts=None):
		'''
		Retrieves up to _limit_ versions of the specified row,
		at or before the specified timestamp (_ts_).  If _ts_ is None
		or not specified, defaults to now.
		'''
		op = Get( Bytes.toBytes(java_str(rowkey)) )
		if ts is not None:
			op.setTimeRange( 0, ts )
		if limit is not None:
			op.setMaxVersions(limit)

		row = self.table.get(op)

		versions = []
		if row is not None and not row.isEmpty():
			for e in row.list():
				col = str(java.lang.String(e.getColumn()))
				colts = e.getTimestamp()
				val = PBUtil.toValue( e.getValue() )
				versions.append( (colts, {col: val} ) )

		return versions

	def scan(self, cols, startrow=None, limit=50, filter=None):
		rows = []
		if startrow is None:
			startrow = HConstants.EMPTY_START_ROW

		scan = Scan(startrow)
		
		if filter is not None:
			scan.setFilter(filter)

		for c in cols:
			scan.addColumn(c)

		scanner = None

		cnt = 0
		try:
			scanner = self.table.getScanner(scan)
			for rec in scanner:
				if limit is not None and cnt >= limit:
					break
			
				rows.append(todict(rec))
				cnt += 1
		finally:
			if scanner is not None:
				try:
					scanner.close()
				except:
					pass

		return rows

	def scan_apply(self, cols, startrow=None, limit=50, filter=None, rowfunc=None):
		rows = []
		if startrow is None:
			startrow = HConstants.EMPTY_START_ROW
		else:
			startrow = Bytes.toBytes( java_str(startrow) )

		scan = Scan(startrow)
		
		if filter is not None:
			scan.setFilter(filter)

		for c in cols:
			scan.addColumn(c)

		cnt = 0
		scanner = None
		try:
			scanner = self.table.getScanner(scan)
			for rec in scanner:
				if limit is not None and cnt >= limit:
					break

				rowfunc(rec)
				cnt += 1
		finally:
			if scanner is not None:
				try:
					scanner.close()
				except:
					pass

		return

	def save(self, rowkey, vals, ts=None):
		key = java_str(rowkey)

		rowup = Put( Bytes.toBytes(key) )
		if ts is not None:
			rowup.setTimestamp(ts)

		for k, v in vals.items():
			(fam, col) = KeyValue.parseColumn( Bytes.toBytes( java_str(k) ) )
			if isinstance(v, com.google.protobuf.Message):
				rowup.add(fam, col, v.toByteArray())
			else:
				rowup.add(fam, col, PBUtil.toBytes(v))

		self.table.put(rowup)
		
		
	def update(self, startrow, wherefilt, sets):
		if startrow is None:
			startrow = HConstants.EMPTY_START_ROW
		elif isinstance(startrow, java.lang.String):
			startrow = Bytes.toBytes(startrow)

		updater = ColumnUpdate(sets)
		cols = self.families()

		cnt = 0
		upcnt = 0

		scan = Scan(startrow)
		for c in cols:
			scan.addColumn(c)

		if wherefilt is not None:
			scan.setFilter(filter)

		scanner = None
		try:
			scanner = self.table.getScanner(scan)
			
			for rec in scanner:
				cnt += 1
				rowup = updater.getUpdate(rec)
				if rowup is not None:
					self.table.commit(rowup)
					upcnt += 1
		finally:
			if scanner is not None:
				try:
					scanner.close()
				except:
					pass

		return upcnt
		
		
	def delete(self, rowkey):
		op = Delete( Bytes.toBytes( java_str(rowkey) ) )
		self.table.delete( op )

	def deleteAll(self, rows):
		for rec in rows:
			if '__key__' in rec:
				op = Delete( Bytes.toBytes( java_str( rec['__key__'] ) ) )
				self.table.delete( op )

	def families(self):
		return [coldesc.getNameAsString()+":" for coldesc in self.table.getTableDescriptor().getFamilies()]


class ColumnUpdate(object):
	def __init__(self, replacements):
		self.replace = replacements
		
	def getUpdate(self, row):
		up = Put(row.getRow())
		for k, v in self.replace.items():
			(fam, col) = KeyValue.parseColumn( Bytes.toBytes( java.lang.String(k) ) )
			if isinstance(v, com.google.protobuf.Message):
				up.add(fam, col, v.toByteArray())
			else:
				up.add(fam, col, PBUtil.toBytes(v))

		return up

		
# ******************** Utility functions

def java_str( basestr ):
	if isinstance(basestr, java.lang.String):
		return basestr

	return java.lang.String(basestr)


def todict( rec ):
	vals = dict()
	vals['__key__'] = str(java.lang.String(rec.getRow()))

	for kv in rec.list():
		key = str(java.lang.String(kv.getColumn(), 'UTF-8'))
		val = PBUtil.toValue( kv.getValue() )
		vals[key] = val

	return vals


def where( *args ):
	filtset = FilterList(FilterList.Operator.MUST_PASS_ALL, java.util.ArrayList())
	for f in args:
		filtset.addFilter(f)

	return filtset
		
def eq(colname, colvalue):
	namebytes = Bytes.toBytes( java.lang.String(colname) )
	if isinstance(colvalue, com.google.protobuf.Message):
		valbytes = colvalue.toByteArray()
	else:
		valbytes = PBUtil.toBytes( colvalue )
	return ColumnMatchFilter( namebytes, ColumnMatchFilter.CompareOp.EQUAL, valbytes )
