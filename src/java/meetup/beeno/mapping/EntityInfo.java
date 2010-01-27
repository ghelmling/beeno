package meetup.beeno.mapping;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import meetup.beeno.HBaseEntity;
import meetup.beeno.HIndex;
import meetup.beeno.HProperty;
import meetup.beeno.HRowKey;
import meetup.beeno.mapping.EntityMetadata.PropertyType;

/**
 * Encapsulates the mapping of an entity class and its properties
 * to an HBase table and columns.
 * 
 * @author garyh
 *
 */
public class EntityInfo {
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