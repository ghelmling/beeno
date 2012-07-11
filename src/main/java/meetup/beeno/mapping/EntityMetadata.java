package meetup.beeno.mapping;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import meetup.beeno.HEntity;
import meetup.beeno.HProperty;
import meetup.beeno.HRowKey;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

/**
 * Central cache for parsed mapping metadata on entity classes.  Given an entity class reference,
 * this will parse out {@link HEntity}. {@link HRowKey}, {@link HProperty} annotations for the class
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
		HEntity classTable = (HEntity) clazz.getAnnotation(HEntity.class);
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
      ColumnQualifier qual = new ColumnQualifier(propAnnotation.family(), propAnnotation.name());
			PropertyDescriptor currentMapped = info.getFieldProperty(qual);
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
}
