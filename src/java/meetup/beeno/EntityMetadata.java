package meetup.beeno;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

/**
 * Central cache for parsed mapping metadata on entity classes.  Given an entity class reference,
 * this will parse out {@link HBaseEntity}. {@link HRowKey}, {@link HProperty} annotations for the class
 * and cache the results for future queries.
 * 
 * @author garyh
 *
 */
public class EntityMetadata {
	
	private static Logger log = Logger.getLogger(EntityMetadata.class.getName());
	
	private static EntityMetadata instance = null;
	
	public static enum PropertyType {
		string(String.class), 
		int_type(Integer.TYPE), 
		float_type(Float.TYPE), 
		double_type(Double.TYPE), 
		long_type(Long.TYPE);
		
		private Class clazz = null;
		
		PropertyType(Class clazz) {	this.clazz = clazz;	}
		public Class getTypeClass() { return this.clazz; }
	};
	
	private Map<Class, EntityInfo> mappings = new ConcurrentHashMap<Class, EntityInfo>();
	private HTablePool pool = new HTablePool();
	
	private EntityMetadata() {
		
	}
	
	/**
	 * Returns the annotated HBase metadata for the given entity class.  If the class is not
	 * yet parsed, it will be parsed and added to the internal mapping.
	 * 
	 * @param entityClass
	 * @return
	 */
	public EntityInfo getInfo(Class entityClass) throws MappingException {
		EntityInfo info = this.mappings.get(entityClass);
		
		if (info == null) {
			info = parseEntity(entityClass);
			mappings.put(entityClass, info);
		}
		
		return info;
	}
	
	
	/**
	 * Reads all mapping annotations from the passed in class, to build up
	 * a set of the HBase table and column associations.
	 * 
	 * @param clazz
	 * @return
	 * @throws MappingException
	 */
	protected EntityInfo parseEntity(Class clazz) throws MappingException {
		// lookup any class mappings
		HBaseEntity classTable = (HBaseEntity) clazz.getAnnotation(HBaseEntity.class);
		if (classTable ==  null) {
			throw new MappingException(clazz, "Not an entity class!");
		}
		EntityInfo info = new EntityInfo(clazz);
		info.setTablename(classTable.name());
				
		// lookup any property mappings for table fields and indexes
		parseProperties(clazz, info);
		
		// make sure we have a mapping for the row key
		if (info.getKeyProperty() == null) {
			throw new MappingException(clazz, "Missing required annotation for HTable row key property");
		}
		
		return info;
	}
	
	
	/**
	 * Examines the java bean properties for the class, looking for column mappings
	 * and a mapping for the row key.
	 * @param clazz
	 * @param info
	 */
	protected void parseProperties(Class clazz, EntityInfo info) throws MappingException {
		try {
			BeanInfo clazzInfo = Introspector.getBeanInfo(clazz);
			
			for (PropertyDescriptor prop : clazzInfo.getPropertyDescriptors()) {
				Method readMethod  = prop.getReadMethod();
				if (readMethod != null) {
					parseMethod(prop, readMethod, info);
				}

				Method writeMethod  = prop.getWriteMethod();
				if (writeMethod != null) {
					parseMethod(prop, writeMethod, info);
				}
			}
		}
		catch (IntrospectionException ie) {
			log.error("Failed to get BeanInfo: "+ie.getMessage());
		}
	}
	
	protected void parseMethod(PropertyDescriptor prop, Method meth, EntityInfo info) 
			throws MappingException {
		// see if this is mapped to the row key -- if so it's not allowed to be a field
		HRowKey key = (HRowKey) meth.getAnnotation(HRowKey.class);
		if (key != null) {
			// check for a duplicate mapping (composite keys not supported)
			if (info.getKeyProperty() != null && !prop.equals(info.getKeyProperty())) {
				throw new MappingException( info.getEntityClass(),
						String.format("Duplicate mappings for table row key: %s, %s", 
									  info.getKeyProperty().getName(), prop.getName()) );
			}
			info.setKeyProperty(prop);
			return;
		}
		
		// check for property mapping
		HProperty propAnnotation = (HProperty) meth.getAnnotation(HProperty.class);
		if (propAnnotation != null) {
			String fieldname = fieldToString(propAnnotation);
			PropertyDescriptor currentMapped = info.getFieldProperty(fieldname);
			// check for a duplicate mapping
			if (currentMapped != null && !prop.equals(currentMapped)) {
				throw new MappingException( info.getEntityClass(),
						String.format("Duplicate mappings for table field: %s, %s", currentMapped.getName(), prop.getName()) );
			}
			String typeName = propAnnotation.type();
			PropertyType type = null;
			if (typeName != null && !"".equals(typeName.trim())) {
				try {
					type = PropertyType.valueOf(typeName);
				}
				catch (IllegalArgumentException iae) {
					throw new MappingException( info.getEntityClass(),
							String.format("Invalid property type ('%s') for '%s'", typeName, prop.getName()) );
				}
			}

			info.addProperty(propAnnotation, prop, type);
		}
		
	}
	
	protected String fieldToString(HProperty prop) {
		StringBuilder builder = new StringBuilder(prop.family()).append(":");
		if (prop.name() != null && !"*".equals(prop.name()))
			builder.append(prop.name());
		
		return builder.toString();
	}
	
	
	/**
	 * Returns the main metadata instance.  Ignoring synchronization here 
	 * as having a few copies initially isn't too bad.
	 * @return
	 */
	public static EntityMetadata getInstance() {
		if (instance == null)
			instance = new EntityMetadata();
		
		return instance;
	}
	
	
	/* *************** Mapping Metadata Classes *************** */
	
	/**
	 * Stores an annotated {@link HProperty} mapping of a JavaBean property
	 * in the entity class to an HBase table column.  This maps a single Java object
	 * instance to a single column.
	 */
	public static class FieldMapping {
		protected String family = null;
		protected String column = null;
		protected String fieldname = null;
		protected PropertyDescriptor beanProperty = null;
		public FieldMapping(HProperty prop, PropertyDescriptor beanProperty) {
			this.family = prop.family();
			this.column = prop.name();
			this.fieldname = this.family+":"+this.column;
			this.beanProperty = beanProperty;
		}

		public boolean matches(String fieldname) { return this.fieldname.equals(fieldname); }
		public PropertyDescriptor getBeanProperty() { return this.beanProperty; }
		public String getFamily() { return this.family; }
		public String getColumn() { return this.column; }
		public String getFieldName() { return this.fieldname; }

		public static FieldMapping get(HProperty prop, PropertyDescriptor beanProperty) {
			if (Map.class.isAssignableFrom(beanProperty.getPropertyType())) {
				return new MapField(prop, beanProperty);
			}
			else if (Collection.class.isAssignableFrom(beanProperty.getPropertyType())) {
				return new ListField(prop, beanProperty);
			}

			return new FieldMapping(prop, beanProperty);
		}
	}
	
	
	/**
	 * Represents an annotated {@link HProperty} mapping of a JavaBean java.util.Collection type property
	 * to multiple indexed columns in an HBase table.  The multiple values will be mapped to columns
	 * based on the value index in the collection:
	 * 		[column family]:[column name]_[index number]
	 * 
	 * Due to the mapping to multiple columns, these property types are not easily covered by HBase 
	 * secondary indexes and should not be used as query criteria.
	 * 
	 * @author garyh
	 *
	 */
	public static class ListField extends FieldMapping {
		protected Pattern fieldRegex = null;
		public ListField(HProperty prop, PropertyDescriptor beanProperty) {
			super(prop, beanProperty);
			this.fieldRegex = Pattern.compile(this.family+":"+this.column+"_\\d+");
		}

		public boolean matches(String fieldname) { return this.fieldRegex.matcher(fieldname).matches(); }
	}

	
	/**
	 * Represents an annotated {@link HProperty} mapping of a JavaBean java.util.Map type property
	 * to multiple columns (one per Map key) in an HBase table.  The Map entries will be mapped to columns
	 * in the specified column family, using the convention:
	 * 		[column family]:[entry key]
	 * 
	 * Like the ListField mapping, these property types cannot easily be indexed using HBase secondary indexes, 
	 * due to the dynamic nature of the column names.  So these properties should not be used as query criteria.
	 * 
	 * @author garyh
	 *
	 */
	public static class MapField extends FieldMapping {
		protected Pattern fieldRegex = null;
		public MapField(HProperty prop, PropertyDescriptor beanProperty) {
			super(prop, beanProperty);
			this.fieldRegex = Pattern.compile(this.family+":.+");
			// fieldname should ignore wildcard pattern
			this.fieldname = this.family + ":";
		}

		public boolean matches(String fieldname) { return this.fieldRegex.matcher(fieldname).matches(); }
	}
	
	/**
	 * Represents an index configuration annotated on an entity property
	 * @author garyh
	 *
	 */
	public static class IndexMapping {
		protected String indexTable;
		protected FieldMapping primaryField;
		protected HUtil.HCol dateCol;
		protected boolean invertDate = false;
		protected List<HUtil.HCol> extraFields = new ArrayList<HUtil.HCol>();
		protected EntityIndexer generator;
		
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
			
			this.generator = new EntityIndexer(this);
		}
		
		public String getTableName() { return this.indexTable; }
		public FieldMapping getPrimaryField() { return this.primaryField; }
		public HUtil.HCol getDateField() { return this.dateCol; }
		public boolean isDateInverted() { return this.invertDate; }
		public List<HUtil.HCol> getExtraFields() { return this.extraFields; }
		public EntityIndexer getGenerator() { return this.generator; }
	}


	/**
	 * Encapsulates the mapping of an entity class and its properties
	 * to an HBase table and columns.
	 * 
	 * @author garyh
	 *
	 */
	public static class EntityInfo {
		private Class entityClass = null;
		private String table = null;
		private PropertyDescriptor keyProperty = null;
		
		private List<FieldMapping> mappedProps = new ArrayList<FieldMapping>();
		private Map<String, PropertyDescriptor> propertiesByName = new HashMap<String, PropertyDescriptor>();
		private Map<PropertyDescriptor, FieldMapping> fieldsByProperty = new HashMap<PropertyDescriptor, FieldMapping>();
		private Map<PropertyDescriptor, PropertyType> typesByProperty = new HashMap<PropertyDescriptor, PropertyType>();
		private Map<PropertyDescriptor, List<IndexMapping>> indexesByProperty = new HashMap<PropertyDescriptor, List<IndexMapping>>();
		
		public EntityInfo(Class clazz) {
			this.entityClass = clazz;
		}
		
		public Class getEntityClass() { return this.entityClass; }
		
		/**
		 * Returns the HBase table identified by the entity's {@link HBaseEntity} 
		 * annotation.
		 * @return
		 */
		public String getTablename() { return this.table; }
		public void setTablename(String tablename) { this.table = tablename; }
		
		/**
		 * Returns the java bean properties mapped by the entity's {@link HRowKey}
		 * annotation.
		 * 
		 * @return
		 */
		public PropertyDescriptor getKeyProperty() { return this.keyProperty; }
		public void setKeyProperty(PropertyDescriptor prop) { this.keyProperty = prop; }
		
		public void addProperty(HProperty mapping, PropertyDescriptor prop, PropertyType type) {
			FieldMapping field = FieldMapping.get(mapping, prop);
			this.mappedProps.add(field);
			this.propertiesByName.put(prop.getName(), prop);
			this.fieldsByProperty.put(prop, field);
			if (type != null)
				this.typesByProperty.put(prop, type);
			
			// !!! ADD HANDLING FOR INDEX ANNOTATIONS !!!
			HIndex[] indexes = mapping.indexes();
			if (indexes != null && indexes.length > 0) {
				for (HIndex idx : indexes)
					addIndex( new IndexMapping(this.table, field, idx), prop );
			}
		}
		
		public void addIndex(IndexMapping index, PropertyDescriptor prop) {
			List<IndexMapping> curIndexes = this.indexesByProperty.get(prop);
			if (curIndexes == null) {
				curIndexes = new ArrayList<IndexMapping>();
				this.indexesByProperty.put(prop, curIndexes);
			}
			
			curIndexes.add(index);
		}
		
		public PropertyDescriptor getFieldProperty(String fieldname) {
			for (FieldMapping mapping : this.mappedProps) {
				if (mapping.matches(fieldname)) {
					return mapping.getBeanProperty();
				}
			}

			return null;
		}
		
		public List<FieldMapping> getMappedFields() { return this.mappedProps; }
		
		public PropertyDescriptor getProperty(String propName) {
			return this.propertiesByName.get(propName);
		}

		public FieldMapping getPropertyMapping(String propName) {
			if (this.propertiesByName.get(propName) != null) {
				return this.fieldsByProperty.get( this.propertiesByName.get(propName) );
			}

			return null;
		}
		
		public PropertyType getPropertyType(PropertyDescriptor prop) {
			return this.typesByProperty.get(prop);
		}
		
		public List<IndexMapping> getPropertyIndexes(String propname) {
			PropertyDescriptor prop = getProperty(propname);
			if (prop != null)
				return getPropertyIndexes(prop);
			
			return null;
		}
		
		public List<IndexMapping> getPropertyIndexes(PropertyDescriptor prop) {
			return this.indexesByProperty.get(prop);
		}
		
		public IndexMapping getFirstPropertyIndex(String propname) {
			PropertyDescriptor prop = getProperty(propname);
			if (prop != null)
				return getFirstPropertyIndex(prop);
			
			return null;
		}
		
		public IndexMapping getFirstPropertyIndex(PropertyDescriptor prop) {
			List<IndexMapping> indexes = getPropertyIndexes(prop);
			if (indexes != null && indexes.size() > 0)
				return indexes.get(0);
			
			return null;
		}
		
		public Map<PropertyDescriptor, List<IndexMapping>> getIndexesByProperty() {
			return this.indexesByProperty;
		}
		
		/**
		 * Returns all index mappings present on this entity class
		 * @return
		 */
		public List<IndexMapping> getMappedIndexes() {
			List<IndexMapping> indexes = new ArrayList<IndexMapping>(this.indexesByProperty.size());
			for (List<IndexMapping> propIndexes : this.indexesByProperty.values())
				indexes.addAll(propIndexes);
			
			return indexes;
		}
		
		public boolean isGettable(String fieldname) {
			PropertyDescriptor prop = getFieldProperty(fieldname);
			return (prop != null && prop.getReadMethod() != null);
		}

		public boolean isSettable(String fieldname) {
			PropertyDescriptor prop = getFieldProperty(fieldname);
			return (prop != null && prop.getWriteMethod() != null);
		}
		
		public Collection<String> getColumnFamilyNames() {
			if (this.columnFamilies == null) {
				Set<String> names = new HashSet<String>();
				for (FieldMapping mapping : this.mappedProps)
					names.add(mapping.getFamily());
				
				this.columnFamilies = names;
			}
			
			return this.columnFamilies;
		}
		private Set<String> columnFamilies = null;
	}
}
