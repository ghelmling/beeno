package meetup.beeno;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import meetup.beeno.mapping.IndexMapping;
import meetup.beeno.util.HUtil;
import meetup.beeno.util.PBUtil;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Generates updates for secondary index tables based on an update for
 * the indexed table.  This implementation generates index keys based
 * on a primary property value (from which the index is mapped) and an
 * optional date column, which can be appended to the key as is or as
 * an inverted value for reverse chronological sorting.
 * 
 * Extra columns mapped in the index will be stored in index table
 * row, along with the primary property value and the date column (if
 * present), and can be used for filtering in scanning the index.
 * 
 * TODO: Currently this implementation does not handle removing
 * secondary index rows when a value is removed!!!
 * 
 * @author garyh
 *
 */
public class EntityIndexer {
	static final byte[] INDEX_FAMILY = Bytes.toBytes("__idx__");
	static final byte[] INDEX_KEY_COLUMN = Bytes.toBytes("row");
	private static final byte[] ROW_KEY_SEP = Bytes.toBytes("-");

	
	private static Logger log = Logger.getLogger(EntityIndexer.class);
	
	private String indexTable;
	private HUtil.HCol primaryField;
	private HUtil.HCol dateField;
	private boolean invertDate = false;
	/** Use the builtin date from HBase versioning in place of a separate column */
	private boolean useBuiltinDate = false;
	private List<HUtil.HCol> extraFields;
	private IndexKeyFactory keyFactory = new DefaultKeyFactory();

	public EntityIndexer(IndexMapping mapping) {
		this.indexTable = mapping.getTableName();
		this.primaryField = new HUtil.HCol(mapping.getPrimaryField().getFamily(),
										   mapping.getPrimaryField().getColumn());
		this.dateField = mapping.getDateField();
		this.invertDate = mapping.isDateInverted();
		this.useBuiltinDate = mapping.useBuiltinDate();
		this.extraFields = mapping.getExtraFields();
		
		if (mapping.getKeyFactory() != null) {
			try {
				this.keyFactory = mapping.getKeyFactory().newInstance();
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Unable to instantiate key factory class", e);
			}
		}
	}
	
	public String getIndexTable() { return this.indexTable; }
	
	/**
	 * Returns a set of updates for this index table, based on the 
	 * update to the underlying table.
	 * 
	 * @param entityUpdate
	 * @return
	 */
	public List<Put> getIndexUpdates(Put entityUpdate) {
		List<Put> up = new ArrayList<Put>(1);

		/* TODO: handle many index records to one base record.
		 * For example, if the primary property value contains 
		 * a collection type, a new index record could be generated
		 * for each value in the collection.
		 */
		Put valUpdate = getUpdateForValue(entityUpdate);
		if (valUpdate != null)
			up.add(valUpdate);
		
		return up;
	}
	
	protected Put getUpdateForValue(Put entityUpdate) {
		Put put = null;
		
		// generate the index row key
		Map<byte[],List<KeyValue>> familyMap = entityUpdate.getFamilyMap();
		// store all the indexed values
		byte[] primaryVal = getValue(this.primaryField.family(), this.primaryField.column(), familyMap);
		if (primaryVal != null && primaryVal.length > 0) {
			Long date = getDateValue(familyMap, entityUpdate);
			put = new Put( createIndexKey(primaryVal, date, entityUpdate.getRow()) );
			
			// sync with base timestamp
			put.setTimeStamp( entityUpdate.getTimeStamp() );
			// store all specified values (when present)
			put.add(this.primaryField.family(), this.primaryField.column(), primaryVal);
			
			if (this.dateField != null && date != null)
				put.add(this.dateField.family(), this.dateField.column(), PBUtil.toBytes(date));
			
			// add any extra fields
			for (HUtil.HCol col : this.extraFields) {
				byte[] val = getValue(col.family(), col.column(), familyMap);
				if (val != null)
					put.add(col.family(), col.column(), val);
			}
			
			// store the orig record key
			put.add(INDEX_FAMILY, INDEX_KEY_COLUMN, entityUpdate.getRow());
		}
		else {
			// no update for primary value, skip
			log.debug("No primary value for index "+getIndexTable());
		}
		return put;
	}
	
	protected Long getDateValue(Map<byte[],List<KeyValue>> familyMap, Put entityUpdate) {
		Long dateVal = null;
		
		if (this.dateField != null) {
			byte[] rawDate = getValue(this.dateField.family(), 
									  this.dateField.column(), familyMap);
			HDataTypes.HField pbDate = PBUtil.readMessage(rawDate);
			// dates are assumed to be Long!
			if (pbDate != null && pbDate.getType() == HDataTypes.HField.Type.INTEGER)
				dateVal = pbDate.getInteger();
		}
		else if (this.useBuiltinDate) {
			dateVal = entityUpdate.getTimeStamp();
		}
		
		return dateVal;
	}
	
	protected byte[] getValue(byte[] family, byte[] col, 
							  Map<byte[],List<KeyValue>> familyMap) {
		byte[] val = null;
		List<KeyValue> familyVals = familyMap.get(family);
		if (familyVals != null) {
			for (KeyValue kv : familyVals) {
				if (kv.matchingColumn(family, col)) {
					val = kv.getValue();
					break;
				}
			}
		}
		return val;
	}
	
	/** 
	 * TODO: split this out into a separate interface for different
	 * implementations
	 */
	public byte[] createIndexKey(byte[] primaryVal, Long date, byte[] origRow) {
		if (date != null) {
			return this.keyFactory.createKey(primaryVal, origRow, date, this.invertDate);
		}
		else {
			return this.keyFactory.createKey(primaryVal, origRow, null, this.invertDate);
		}
	}
	
	
	public static class DefaultKeyFactory implements IndexKeyFactory {

		@Override
		public byte[] createKey( byte[] primaryVal, byte[] rowKey, Long date, boolean invertDate ) {
			byte[] key = new byte[0];
			HDataTypes.HField pbVal = PBUtil.readMessage(primaryVal);
			// order numeric types
			if (pbVal != null && pbVal.getType() == HDataTypes.HField.Type.INTEGER) {
				key = Bytes.add(key, HUtil.toOrderedBytes(pbVal.getInteger()));
			}
			else {
				// just use raw bytes
				key = Bytes.add(key, primaryVal);
			}
			
			// add on date, if specified
			if (date != null) {
				key = Bytes.add(key, 
							Bytes.add(ROW_KEY_SEP, HUtil.toOrderedBytes(date, invertDate)) );
			}
			
			// add on the original row key to ensure uniqueness
			if (rowKey != null && rowKey.length > 0) {
				key = Bytes.add(key, 
						Bytes.add(ROW_KEY_SEP, rowKey));
			}
			
			return key;
		}	
	}
	
	
	/**
	 * Generates the same index keys as DefaultKeyFactory, but prefixed with the primary value mod 100 for
	 * better row key distribution.
	 * 
	 * This is designed specifically to avoid hot regions arising from frequently used indexes based off of
	 * a sequentially incremented primary value.
	 * @author garyh
	 *
	 */
	public static class ModKeyFactory extends DefaultKeyFactory {
		private static int base = 100;
		
		public byte[] createKey( byte[] primaryVal, byte[] rowKey, Long date, boolean invertDate) {
			byte[] key = new byte[0];
			HDataTypes.HField pbVal = PBUtil.readMessage(primaryVal);
			// order numeric types
			if (pbVal != null && pbVal.getType() == HDataTypes.HField.Type.INTEGER) {
				long val = pbVal.getInteger();
				key = Bytes.add(Bytes.toBytes(Long.toString( val % base )), ROW_KEY_SEP); 
			}
			key = Bytes.add(key, super.createKey(primaryVal, rowKey, date, invertDate));
			
			return key;
		}
	}
}
