package meetup.beeno;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HUtil {
	// max number of digits in a long value
	private static final int LONG_CHAR_LENGTH = 19;
	private static byte[] ZERO_FILL = new byte[LONG_CHAR_LENGTH];
	static {
		Arrays.fill(ZERO_FILL, (byte)'0');
	}
	
	// FIXME: this shouldn't be static and should be configurable
	private static final int MAX_POOL_SIZE = 100;

	private static Logger log = Logger.getLogger(HUtil.class.getName());
	
	private static HTablePool pool = new HTablePool(new HBaseConfiguration(), MAX_POOL_SIZE);
	
	public static HTable getTable(String tablename) {
		if (log.isDebugEnabled())
			log.debug("Getting table "+tablename+" from pool");
		return pool.getTable(tablename);
	}
	
	public static void releaseTable(HTable table) {
		if (table != null) {
			if (log.isDebugEnabled())
				log.debug("Returning table "+Bytes.toString(table.getTableName())+" to pool");
			pool.putTable(table);
		}
	}

	/**
	 * Returns the column name portion of a HBase column description
	 * in the form "family:column"
	 * 
	 * @param fullColumn
	 * @return
	 */
	public static String column(String fullColumn) {
		int sepIdx = fullColumn.indexOf(":");
		if (sepIdx >= 0 && sepIdx < (fullColumn.length()-1))
			return fullColumn.substring(sepIdx+1);
		
		return fullColumn;
	}

	/**
	 * Returns the column family portion of a HBase column description
	 * in the form "family:column"
	 * 
	 * @param fullColumn
	 * @return
	 */
	public static String family(String fullColumn) {
		int sepIdx = fullColumn.indexOf(":");
		if (sepIdx > 0)
			return fullColumn.substring(0, sepIdx);

		return null;
	}

	/**
	 * Attempts to convert the byte array value to the specified type
	 */
	public static Object convertValue(byte[] val, Class propType) {
		if (val == null && propType == null) {
			return null;
		}
		
		if (propType == Integer.TYPE || propType == Integer.class) {
			return Bytes.toInt(val);
		}
		else if (propType == Float.TYPE || propType == Float.class) {
			return Bytes.toFloat(val);
		}
		else if (propType == Double.TYPE || propType == Double.class) {
			return Bytes.toDouble(val);
		}
		else if (propType == Long.TYPE || propType == Long.class) {
			return Bytes.toLong(val);
		}
		else if (propType == String.class) {
			return Bytes.toString(val);
		}
		else if (propType == Date.class) {
			// FIXME: no timezone handling
			return new Date(Bytes.toLong(val));
		}
		else if (Enum.class.isAssignableFrom(propType)) {
			String enumName = null;
			try {
				enumName = Bytes.toString(val);
				if (enumName == null)
					return null;
				return Enum.valueOf(propType, enumName.toUpperCase());
			}
			catch (IllegalArgumentException iae) {
				log.error(String.format("Invalid enum constant '%s' for class %s", enumName, propType.getName()));
				return null;
			}
		}
		else {
			// not handled
			log.warn(String.format("Unknown conversion for property type %s", propType.getName()));
			return val;
		}
	}

	/**
	 * Attempts to convert the value object to a byte array for storage
	 * @param val
	 * @return
	 */
	public static byte[] convertToBytes(Object val) {
		if (val == null) {
			return null;
		}
		
		if (val.getClass().isArray() && val.getClass().getComponentType() == Byte.TYPE) {
			return (byte[])val;
		}
		else if (val instanceof Integer) {
			return Bytes.toBytes( ((Integer)val) );
		}
		else if (val instanceof Float) {
			return Bytes.toBytes(((Float)val));
		}
		else if (val instanceof Double) {
			return Bytes.toBytes(((Double)val));
		}
		else if (val instanceof Long) {
			return Bytes.toBytes(((Long)val));
		}
		else if (val instanceof String) {
			return Bytes.toBytes(((String)val));
		}
		else if (val instanceof Date) {
			// FIXME: no timezone handling
			return Bytes.toBytes(((Date)val).getTime());
		}
		else if (val instanceof Enum) {
			return Bytes.toBytes(((Enum)val).name().toLowerCase());
		}
		else {
			// not handled
			log.warn(String.format("Unknown conversion to bytes for property value type %s", val.getClass().getName()));
			return null;
		}
	}
	
	
	public static String[] getMappedFamilies(EntityMetadata.EntityInfo info) {
		Set<String> families = new HashSet<String>();
		for (EntityMetadata.FieldMapping field : info.getMappedFields()) {
			families.add(field.getFamily()+":");
		}
		
		return families.toArray(new String[0]);
	}


	public static byte[] toOrderedBytes(Long ts) {
		return toOrderedBytes(ts, false);
	}
	
	public static byte[] toOrderedBytes(Long ts, boolean invert) {
		long idxval = (invert ? (Long.MAX_VALUE - ts) : ts);
		byte[] valbytes = Bytes.toBytes( Long.toString(idxval) );
		byte[] paddedbytes = new byte[LONG_CHAR_LENGTH];
		int padchars = LONG_CHAR_LENGTH - valbytes.length;
		if (padchars > 0) {
			System.arraycopy(ZERO_FILL, 0, paddedbytes, 0, padchars);
		}
		System.arraycopy(valbytes, 0, paddedbytes, padchars, valbytes.length);
		return paddedbytes;
	}
	
	
	public static Object cast(Object val, Class type) throws ClassCastException {
		if (val == null)
			return null;

		if (type == Boolean.class || type == Boolean.TYPE) {
			return ((Boolean)val).booleanValue();
		}
		else if (type == Character.class || type == Character.TYPE) {
			return ((Character)val).charValue();
		}
		else if (type == Byte.class || type == Byte.TYPE) {
			return ((Byte)val).byteValue();
		}
		else if (type == Short.class || type == Short.TYPE) {
			if (val instanceof Long)
				return ((Long)val).shortValue();
			else if (val instanceof Integer)
				return ((Integer)val).shortValue();
			else if (val instanceof Short)
				return ((Short)val).shortValue();
		}
		else if (type == Integer.class || type == Integer.TYPE) {
			if (val instanceof Long)
				return ((Long)val).intValue();
			else if (val instanceof Integer)
				return ((Integer)val).intValue();
		}
		else if (type == Long.class || type == Long.TYPE) {
			if (val instanceof Integer)
				return ((Integer)val).longValue();
			else if (val instanceof Long)
				return ((Long)val).longValue();
		}
		else if (type == Float.class || type == Float.TYPE) {
			if (val instanceof Double)
				return ((Double)val).floatValue();
			else if (val instanceof Float)
				return ((Float)val).floatValue();
		}
		else if (type == Double.class || type == Double.TYPE) {
			if (val instanceof Double)
				return ((Double)val).doubleValue();
			else if (val instanceof Float)
				return ((Float)val).doubleValue();
		}
		else if (type.isAssignableFrom(val.getClass())) {
			return type.cast(val);
		}
		
		return null;
	}

	
	public static class HCol {
		private byte[] family;
		private byte[] column;
		
		public HCol(String familyName, String columnName) {
			this(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		}
		
		public HCol(byte[] familyBytes, byte[] columnBytes) {
			this.family = familyBytes;
			this.column = columnBytes;
		}
		
		public byte[] family() { return this.family; }
		public byte[] column() { return this.column; }
		
		public static HCol parse(String fullName) {
			byte[][] parts = KeyValue.parseColumn(Bytes.toBytes(fullName));
			if (parts == null || parts.length != 2)
				return null;
			
			return new HCol(parts[0], parts[1]);
		}
	}
}