package meetup.beeno;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import meetup.beeno.HBaseEntity;
import meetup.beeno.HIndex;
import meetup.beeno.HProperty;
import meetup.beeno.HRowKey;

/**
 * Collection of sample entity mappings for testing.
 * 
 * @author garyh
 *
 */
public class TestEntities {
	/**
	 * Simple, happy path entity with all the right annotations
	 * @author garyh
	 *
	 */
	@HBaseEntity(name="test_simple")
	public static class SimpleEntity {
		String id;
		String stringProperty;
		int intProperty;
		float floatProperty;
		double doubleProperty;
		long longProperty;

		public SimpleEntity() {
		}

		public SimpleEntity(String id) {
			this.id = id;
		}

		public SimpleEntity(String id,
							String stringProp,
							int intProp,
							float floatProp,
							double doubleProp,
							long longProp) {
			this.id = id;
			this.stringProperty = stringProp;
			this.intProperty = intProp;
			this.floatProperty = floatProp;
			this.doubleProperty = doubleProp;
			this.longProperty = longProp;
		}

		@HRowKey
		public String getId() { return this.id; }
		public void setId(String id) { this.id = id; }

		@HProperty(family="props", name="stringcol")
		public String getStringProperty() { return stringProperty; }
		public void setStringProperty( String stringProperty ) {
			this.stringProperty = stringProperty;
		}

		@HProperty(family="props", name="intcol")
		public int getIntProperty() { return intProperty; }
		public void setIntProperty( int intProperty ) {
			this.intProperty = intProperty;
		}

		@HProperty(family="props", name="floatcol")
		public float getFloatProperty() { return floatProperty; }
		public void setFloatProperty( float floatProperty ) {
			this.floatProperty = floatProperty;
		}

		@HProperty(family="props", name="doublecol")
		public double getDoubleProperty() { return doubleProperty; }
		public void setDoubleProperty( double doubleProperty ) {
			this.doubleProperty = doubleProperty;
		}

		@HProperty(family="props", name="longcol")
		public long getLongProperty() { return longProperty; }
		public void setLongProperty( long longProperty ) {
			this.longProperty = longProperty;
		}
	}

	/**
	 * More complex entity class containing mapped collections
	 * @author garyh
	 */
	@HBaseEntity(name="test_complex")
	public static class ComplexEntity {
		String id;
		List<String> stringList = new ArrayList<String>();
		Set<Integer> intSet = new HashSet<Integer>();
		Map<String,String> extendedMap = new HashMap<String,String>();

		public ComplexEntity() {}

		public ComplexEntity(String id) {
			this.id = id;
		}

		public ComplexEntity(String id, List<String> strings, Set<Integer> ints) {
			this.id = id;
			this.stringList = strings;
			this.intSet = ints;
		}

		@HRowKey
		public String getId() { return this.id; }
		public void setId(String id) { this.id = id; }

		@HProperty(family="props", name="strings", type="string")
		public List<String> getStringList() { return this.stringList; }
		public void setStringList(List<String> list) { this.stringList = list; }

		@HProperty(family="props", name="ints", type="int_type")
		public Set<Integer> getIntSet() { return this.intSet; }
		public void setIntSet(Set<Integer> set) { this.intSet = set; }

		@HProperty(family="extended", name="*", type="string")
		public Map<String, String> getExtendedProps() {
			return this.extendedMap;
		}
		public void setExtendedProps(Map<String,String> props) {
			this.extendedMap = props;
		}
	}

	@HBaseEntity(name="test_nokey")
	public static class NoKeyEntity {
		String prop1;

		@HProperty(family="props", name="prop1")
		public String getProp1() { return this.prop1; }
		public void setProp1(String prop) { this.prop1 = prop; }
	}

	@HBaseEntity(name="test_dupekey")
	public static class DupeKeyEntity {
		String id1;
		String id2;

		@HRowKey
		public String getId1() { return this.id1; }
		@HRowKey
		public String getId2() { return this.id2; }
	}

	@HBaseEntity(name="test_dupefield")
	public static class DupeFieldEntity {
		String id;
		String prop1;
		String prop2;

		@HRowKey
		public String getId() { return this.id; }
		@HProperty(family="info", name="col1")
		public String getProp1() { return this.prop1; }
		@HProperty(family="info", name="col1")
		public String getProp2() { return this.prop2; }
	}

	/**
	 * Simple, happy path entity with all the right annotations
	 * @author garyh
	 *
	 */
	@HBaseEntity(name="test_indexed")
	public static class IndexedEntity {
		String id;
		String stringProperty;
		Integer intKey;
		long timestamp;

		public IndexedEntity() {
		}

		public IndexedEntity(String id) {
			this.id = id;
		}

		public IndexedEntity(String id,
							 String stringProp,
							 Integer intKey,
							 long timestamp) {
			this.id = id;
			this.stringProperty = stringProp;
			this.intKey = intKey;
			this.timestamp = timestamp;
		}

		@HRowKey
		public String getId() { return this.id; }
		public void setId(String id) { this.id = id; }

		@HProperty(family="props", name="stringcol",
				   indexes = { @HIndex(extra_cols={"props:tscol"}) } )
		public String getStringProperty() { return stringProperty; }
		public void setStringProperty( String stringProperty ) {
			this.stringProperty = stringProperty;
		}

		@HProperty(family="props", name="intcol",
				   indexes = { @HIndex(date_col="props:tscol", date_invert=true, extra_cols={"props:tscol"}) } )
		public Integer getIntKey() { return intKey; }
		public void setIntKey( Integer val ) {
			this.intKey = val;
		}

		@HProperty(family="props", name="tscol")
		public long getTimestamp() { return timestamp; }
		public void setTimestamp( long ts ) { this.timestamp = ts; }
	}
}
