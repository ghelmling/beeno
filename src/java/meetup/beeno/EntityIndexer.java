package meetup.beeno;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
	private List<HUtil.HCol> extraFields;

	public EntityIndexer(EntityMetadata.IndexMapping mapping) {
		this.indexTable = mapping.getTableName();
		this.primaryField = new HUtil.HCol(mapping.getPrimaryField().getFamily(),
										   mapping.getPrimaryField().getColumn());
		this.dateField = mapping.getDateField();
		this.invertDate = mapping.isDateInverted();
		this.extraFields = mapping.getExtraFields();
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
			Long date = getDateValue(familyMap);
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
	
	protected Long getDateValue(Map<byte[],List<KeyValue>> familyMap) {
		Long dateVal = null;
		
		if (this.dateField != null) {
			byte[] rawDate = getValue(this.dateField.family(), 
									  this.dateField.column(), familyMap);
			HDataTypes.HField pbDate = PBUtil.readMessage(rawDate);
			// dates are assumed to be Long!
			if (pbDate != null && pbDate.getType() == HDataTypes.HField.Type.INTEGER)
				dateVal = pbDate.getInteger();
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
		if (this.dateField != null && date != null) {
			key = Bytes.add(key, 
						Bytes.add(ROW_KEY_SEP, 
								  HUtil.toOrderedBytes(date, this.invertDate)) );
		}
		
		// add on the original row key to ensure uniqueness
		if (origRow != null && origRow.length > 0) {
			key = Bytes.add(key, 
					Bytes.add(ROW_KEY_SEP, origRow));
		}
		
		return key;
	}	
}
