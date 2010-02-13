package meetup.beeno.mapping;

import java.util.ArrayList;
import java.util.List;

import meetup.beeno.EntityIndexer;
import meetup.beeno.HIndex;
import meetup.beeno.IndexKeyFactory;
import meetup.beeno.util.HUtil;
import meetup.beeno.util.HUtil.HCol;

/**
 * Represents an index configuration annotated on an entity property
 * @author garyh
 *
 */
public class IndexMapping {
	protected String indexTable;
	protected FieldMapping primaryField;
	protected HUtil.HCol dateCol;
	protected boolean invertDate = false;
	protected List<HUtil.HCol> extraFields = new ArrayList<HUtil.HCol>();
	protected EntityIndexer generator;
	protected Class<? extends IndexKeyFactory> keyFactory;
	
	public IndexMapping(String baseTable, FieldMapping baseField, HIndex indexAnnotation) {
		this.indexTable = String.format("%s-by_%s", baseTable, baseField.getColumn());
		this.primaryField = baseField;
		for (String col : indexAnnotation.extra_cols()) {
			HUtil.HCol hcol = HUtil.HCol.parse(col);
			if (hcol != null)
				this.extraFields.add( hcol );
		}
		
		if (indexAnnotation.date_col() != null && indexAnnotation.date_col().length() > 0)
			this.dateCol = HUtil.HCol.parse(indexAnnotation.date_col());
		this.invertDate = indexAnnotation.date_invert();
		this.keyFactory = indexAnnotation.key_factory();
		
		this.generator = new EntityIndexer(this);
	}
	
	public String getTableName() { return this.indexTable; }
	public FieldMapping getPrimaryField() { return this.primaryField; }
	public HUtil.HCol getDateField() { return this.dateCol; }
	public boolean isDateInverted() { return this.invertDate; }
	public List<HUtil.HCol> getExtraFields() { return this.extraFields; }
	public EntityIndexer getGenerator() { return this.generator; }
	public Class<? extends IndexKeyFactory> getKeyFactory() { return this.keyFactory; }
}