package meetup.beeno;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import meetup.beeno.mapping.EntityInfo;
import meetup.beeno.mapping.EntityMetadata;
import meetup.beeno.mapping.FieldMapping;
import meetup.beeno.mapping.IndexMapping;
import meetup.beeno.mapping.MappingException;
import meetup.beeno.util.HUtil;
import meetup.beeno.util.PBUtil;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Parameterized class to handle basic data access requirements for HBase mapped entities.
 * 
 * TODO: With the update to 0.20, we're no longer clearing values when an entity field is set
 * to NULL!!!  This is a major flaw in the generic implementation,  but is not a usage we really
 * have right now.  Our previous approach was brute-force -- treat each null value as the delete,
 * which would still be an option if necessary.  But it would be much more efficient to only do 
 * differential changes or updates -- only do a new Put when a new value is set and only do a Delete
 * when an existing (stored) value is actually removed.  But this gets us into more complex territory,
 * like handling the entity classes through a dynamic proxy.  Ugh.  Or else we change the implementation
 * from the simple POJO annotated classes to wrapping values in Property classes which can flag state
 * changes.  Again, ugh. 
 * 
 * @author garyh
 *
 * @param <T>
 */
public class EntityService<T> {

	private static Logger log = Logger.getLogger(EntityService.class.getName());
	
	/** Default collection types to use for generic instances */
	private static Map<Class,Class> defaultCollections = new HashMap<Class,Class>();
	static {
		defaultCollections.put(Collection.class, ArrayList.class);
		defaultCollections.put(List.class, ArrayList.class);
		defaultCollections.put(Map.class, HashMap.class);
		defaultCollections.put(Set.class, HashSet.class);
		defaultCollections.put(SortedSet.class, TreeSet.class);
	}
	
	protected Class<T> clazz;
	private EntityInfo defaultInfo;
	
	/**
	 * Crappy duplication of class parameter to work around type erasure.
	 * @param clazz
	 */
	public EntityService(Class<T> clazz) {
		this.clazz = clazz;
	}

	/**
	 * Always use this access to get the entity info.  The info instance is lazily
	 * instantiated so we don't block on HTable operations (to scan metadata) 
	 * unnecessarily.
	 */
	protected EntityInfo getInfo() throws MappingException {
		if (this.defaultInfo == null) {
			this.defaultInfo = EntityMetadata.getInstance().getInfo(this.clazz);
		}

		return this.defaultInfo;
	}
	
	/**
	 * Returns a single entity instance matching the given row key.  If no matching row is found, returns NULL.
	 * @param rowKey
	 * @return
	 * @throws HBaseException
	 */
	public T get(String rowKey) throws HBaseException {
		T entity = null;
		EntityInfo info = getInfo();
		HTable table = null;
		try {
			table = HUtil.getTable( info.getTablename() );
			Get get = new Get(Bytes.toBytes(rowKey));
			Result row = table.get(get);
			if (row == null || row.isEmpty()) {
				log.info(String.format("%s: row not found for key '%s'", info.getTablename(), rowKey));
			}
			else {
				entity = createFromRow(row);
			}
		}
		catch (IOException ioe) {
			throw new HBaseException(ioe);
		}
		finally {
			HUtil.releaseTable(table);
		}

		return entity;		
	}
	
	
	/**
	 * Instantiates a new entity class instance, and populates the instance with data from the 
	 * passed in HBase RowResult.
	 * 
	 * @param row
	 * @return
	 * @throws HBaseException
	 */
	public T createFromRow(Result row) throws HBaseException {
		T entity = null;
		if (row != null && !row.isEmpty()) {
			try {
				long t1 = System.nanoTime();
				entity = newEntityInstance(row);
				populate(entity, row);
				long t2 = System.nanoTime();
				if (log.isDebugEnabled())
					log.debug(String.format("HBASE TIMER: Created %s entity in %f msec", entity.getClass().getSimpleName(), ((t2-t1)/1000000.0)));
			}
			catch (Exception iae) {
				log.error(String.format("Error instantiating entity %s:", this.clazz.getName()), iae);
			}
		}

		return entity;
	}
	
	/**
	 * Returns a new entity instance.  Separated out so subclasses can look at the row result 
	 * info in determining what entity class to instantiate.
	 * 
	 * @param row
	 * @return
	 * @throws Exception
	 */
	protected T newEntityInstance(Result row) throws Exception {
		return this.clazz.newInstance();
	}
	
	/**
	 * Populate the entity's data fields using reflection.
	 * 
	 * @param entity
	 * @param row
	 */
	public void populate(T entity, Result res) throws HBaseException {
		// set the row key
		EntityInfo info = EntityMetadata.getInstance().getInfo(entity.getClass());
		PropertyDescriptor keyProp = info.getKeyProperty();
		writeProperty(entity, keyProp, res.getRow(), false);
		
		
		Map<PropertyDescriptor,Object> collectionProps = new HashMap<PropertyDescriptor,Object>();
		for (KeyValue kv : res.list()) {
			String col = Bytes.toString(kv.getColumn());
			if (log.isDebugEnabled())
				log.debug(String.format("populate(): column=%s", col));
			
			PropertyDescriptor prop = info.getFieldProperty(col);
			EntityMetadata.PropertyType propType = info.getPropertyType(prop);
			byte[] fieldData = kv.getValue();
			if (prop == null) {
				log.warn(String.format("No entity property mapped for column '%s'", col));
			}
			else if ( Map.class.isAssignableFrom(prop.getPropertyType()) ) {
				Map propVals = (Map) collectionProps.get(prop);
				if (propVals == null) {
					propVals = (Map) newCollectionInstance(prop.getPropertyType());
					collectionProps.put(prop, propVals);
				}

				propVals.put(HUtil.column(col), 
							 PBUtil.toValue(fieldData));
			}
			else if ( Collection.class.isAssignableFrom(prop.getPropertyType()) ) {
				Collection propVals = (Collection) collectionProps.get(prop);
				if (propVals == null) {
					propVals = (Collection) newCollectionInstance(prop.getPropertyType());
					collectionProps.put(prop, propVals);
				}
			
				propVals.add(PBUtil.toValue(fieldData));
			}
			else {
				writeProperty(entity, prop, fieldData);
			}
		}

		// add on mapped collections
		for (PropertyDescriptor prop : collectionProps.keySet()) {
			setProperty(entity, prop, collectionProps.get(prop));
		}
	}


	protected Object newCollectionInstance(Class typeClass) throws HBaseException {
		if (defaultCollections.get(typeClass) != null) {
			typeClass = defaultCollections.get(typeClass);
		}
			
		try {
			return typeClass.newInstance();
		}
		catch (Exception e) {
			throw new HBaseException("Error creating collection for property", e);
		}
	}

	
	/**
	 * Constructs a new query instance for this entity type to create criteria queries
	 * 
	 * @return
	 * @throws MappingException
	 */
	public Query<T> query() throws MappingException {
		return query(this.clazz, null);
	}
	
	/**
	 * Constructs a new query instance for this entity type to create criteria queries
	 * 
	 * @return
	 * @throws MappingException
	 */
	public Query<T> query(QueryOpts opts) throws MappingException {
		return query(this.clazz, opts);
	}

	/**
	 * Constructs a new query instance for this entity type to create criteria queries
	 * 
	 * @return
	 * @throws MappingException
	 */
	public Query<T> query(Class<? extends T> entityClass, QueryOpts opts) throws MappingException {
		Query query = new Query(this, entityClass, opts);
		
		return query;
	}

	/**
	 * Saves the given entity instance into a row in the entity's mapped HTable.
	 * @param entity
	 * @throws HBaseException
	 */
	public void save(T entity) throws HBaseException {
		Put update = getUpdateForEntity(entity);
		EntityInfo info = EntityMetadata.getInstance().getInfo(entity.getClass());
		
		// commit the update
		List<Put> puts = new ArrayList<Put>(1);
		puts.add(update);
		processUpdates(info.getTablename(), puts);

		index(update, info);
	}


	/**
	 * FIXME: does not remove references to the row from index tables!!!!
	 * 
	 * @param rowKey
	 * @throws HBaseException
	 */
	public void delete(String rowKey) throws HBaseException {
		EntityInfo info = getInfo();

		// commit the update
		HTable table = null;
		try {
			table = HUtil.getTable(info.getTablename());
			Delete op = new Delete( Bytes.toBytes(rowKey) );
			table.delete(op);
			
			if (log.isDebugEnabled())
				log.debug(String.format("Committed delete for key '%s'", rowKey));
		}
		catch (IOException ioe) {
			throw new HBaseException(String.format("Error deleting row for key '%s'", rowKey), ioe);
		}
		finally {
			HUtil.releaseTable(table);
		}
	}

	
	/**
	 * Updates any indexes based on entity annotations for the instance
	 */
	public void index(Put update, EntityInfo info) throws HBaseException {
		List<Put> uplist = new ArrayList<Put>(1);
		uplist.add(update);
		index(uplist, info);
	}
	
	
	/**
	 * Updates any indexes based on entity annotations for the instance
	 */
	public void index(List<Put> updates, EntityInfo info) throws HBaseException {
		if (updates == null || updates.size() == 0 || info == null) {
			log.info("Updates or EntityInfo is NULL!");
			return;
		}
		log.info("Calling index for entity "+info.getEntityClass().getName());
		
		List<IndexMapping> indexes = info.getMappedIndexes();
		if (indexes != null && indexes.size() > 0) {
			Map<String,List<Put>> updatesByTable = new HashMap<String,List<Put>>();
			for (IndexMapping idx : indexes) {
				
				EntityIndexer indexer = idx.getGenerator();
				if (indexer != null) {
					for (Put update : updates) {
						List<Put> indexUpdates = indexer.getIndexUpdates(update);
						if (indexUpdates != null && indexUpdates.size() > 0) {
							List<Put> tableUpdates = updatesByTable.get( indexer.getIndexTable() );
							if (tableUpdates == null)
								tableUpdates = new ArrayList<Put>();
							
							tableUpdates.addAll(indexUpdates);
							updatesByTable.put(indexer.getIndexTable(), tableUpdates);
						}
					}					
				}
			}

			// process updates for each table
			int indexCnt = 0;
			for (Map.Entry<String,List<Put>> entry : updatesByTable.entrySet())
				indexCnt += processUpdates(entry.getKey(), entry.getValue());
			
			log.info(String.format("Processed %d index updates for %d entity row(s)", indexCnt, updates.size()));			
		}
		else {
			log.info(String.format("No indexes mapped for entity %s", info.getEntityClass().getName()));
		}
	}
	
	/**
	 * Simple utility to handle batch updates against a table, then correctly
	 * returning the table to the instance pool.
	 * 
	 * @param table
	 * @param updates
	 * @return
	 * @throws HBaseException
	 */
	protected int processUpdates(String table, List<Put> updates)
		throws HBaseException {

		HTable ht = null;
		try {
			ht = HUtil.getTable(table);
			ht.put(updates);
			
			log.info(String.format("Committed %d updates for table %s", updates.size(), Bytes.toString(ht.getTableName())));
		}
		catch (IOException ioe) {
			throw new HBaseException(String.format("IO Error saving updates for table [%s]", table), ioe);
		}
		finally {
			HUtil.releaseTable(ht);
		}
		
		return updates.size();
	}
	
	/**
	 * Commits a number of entity inserts or updates to the table at once.
	 * @param entities
	 * @throws HBaseException
	 */
	public void saveAll(List<T> entities) throws HBaseException {
		if (entities == null || entities.size() == 0)
			return;
		
		List<Put> updates = new ArrayList<Put>(entities.size());
		EntityInfo info = null;
		for (T entity : entities) {
			if (info == null)
				info = EntityMetadata.getInstance().getInfo(entity.getClass());
			updates.add( getUpdateForEntity(entity) );
		}
		
		// commit the update
		processUpdates(getInfo().getTablename(), updates);

		index(updates, info);
	}
	
	
	/**
	 * Applies an update operation to all items returned by the query
	 * @param entity
	 * @return
	 * @throws HBaseException
	 */
	public int update(Query query, EntityUpdate<T> updater) throws HBaseException {
		List<T> items = query.execute();
		List<T> tosave = new ArrayList<T>(items.size());
		for (T item : items) {
			T newitem = updater.update(item);
			if (newitem != null)
				tosave.add(newitem);
		}
		
		saveAll(tosave);
		log.info(String.format("Updated %d items", tosave.size()));
		
		return tosave.size();	
	}
	
	protected Put getUpdateForEntity(T entity) throws HBaseException {
		// get the row key for the update
		EntityInfo entityInfo = EntityMetadata.getInstance().getInfo(entity.getClass());
		PropertyDescriptor keyprop = entityInfo.getKeyProperty();
		// row keys are _not_ encoded as proto bufs
		byte[] rowKey = readProperty(entity, keyprop, false);
		
		if (rowKey == null) {
			// TODO: allow auto-generation of key values
			throw new HBaseException("Cannot save entity with an empty row key");
		}
		
		Put update = new Put(rowKey);
		
		// setup each field
		for (FieldMapping field : entityInfo.getMappedFields()) {
			PropertyDescriptor prop = field.getBeanProperty();
			String fieldname = field.getColumn();
			// allow multiple values for collections
			if (Map.class.isAssignableFrom(prop.getPropertyType())) {
				Map propValues = (Map) getProperty(entity, prop);
				if (propValues != null) {
					for (Object key : propValues.keySet()) {
						String mapfield = fieldname + key.toString();
						setUpdateField(update, field.getFamily(), mapfield, PBUtil.toBytes(propValues.get(key)));
					}
				}
				else {
					// FIXME: delete mapped values
				}
			}
			else if (Collection.class.isAssignableFrom(prop.getPropertyType())) {
				Collection propValues = (Collection) getProperty(entity, prop);
				if (propValues != null) {
					int idx = 0;
					for (Object val : propValues) {
						String indexfield = String.format("%s_%d", fieldname, idx++);
						setUpdateField(update, field.getFamily(), indexfield, PBUtil.toBytes(val));
					}
				}
				else {
					// FIXME: delete all cell values
					setUpdateField(update, field.getFamily(), fieldname, null);
				}
			}
			else {
				byte[] propVal = readProperty(entity, prop);
				setUpdateField(update, field.getFamily(), fieldname, propVal);
			}
		}
		
		return update;
	}
	
	
	protected void setUpdateField(Put update, String family, String column, byte[] propVal) {
		if (propVal == null) {
			// null values indicate a cleared field
			update.add(Bytes.toBytes(family), Bytes.toBytes(column), new byte[0]);
		}
		else {
			update.add(Bytes.toBytes(family), Bytes.toBytes(column), propVal);
		}		
	}
	
	
	/* ========== Utilities to read and write property values ========== */
	
	/**
	 * Reads a bean property's value and returns it as a byte[]
	 */
	protected byte[] readProperty(T entity, PropertyDescriptor prop) throws HBaseException {
		return readProperty(entity, prop, true);
	}
	
	/**
	 * Reads a bean property's value and returns it as a byte[]
	 */
	protected byte[] readProperty(T entity, PropertyDescriptor prop, boolean pbEncode) throws HBaseException {
		if (pbEncode)
			return PBUtil.toBytes( getProperty(entity, prop) );
		else
			return HUtil.convertToBytes( getProperty(entity, prop) );
	}
	
	/**
	 * Wraps calling the property read method
	 * @param entity
	 * @param prop
	 * @return
	 * @throws HBaseException
	 */
	protected Object getProperty(T entity, PropertyDescriptor prop) throws HBaseException {
		Object result = null;
		
		if (prop.getReadMethod() == null) {
			log.warn(String.format("Bean property %s is write-only", prop.getName()));
		}
		else {
			Method getter = prop.getReadMethod();
			try {
				result = getter.invoke(entity);				
			}
			catch (InvocationTargetException exc) {
				log.error(String.format("Error calling property getter: %s.%s", entity.getClass().getName(), getter.getName()), exc);
				throw new HBaseException("Unable to read entity", exc);
			}
			catch (IllegalAccessException iae) {
				log.error(String.format("Error calling property getter: %s.%s", entity.getClass().getName(), getter.getName()), iae);					
				throw new HBaseException("Unable to read entity", iae);
			}
		}
		
		return result;		
	}
	
	/**
	 * Writes a bean property's value in the entity instance, converting
	 * from a byte[] to the property type
	 */
	protected void writeProperty(T entity, PropertyDescriptor prop, byte[] value) 
		    throws HBaseException {
		writeProperty(entity, prop, value, true);
	}
	
	/**
	 * Writes a bean property's value in the entity instance, converting
	 * from a byte[] to the property type
	 */
	protected void writeProperty(T entity, PropertyDescriptor prop, byte[] value, boolean pbEncoded) 
		    throws HBaseException {
		if (pbEncoded)
			setProperty(entity, prop, PBUtil.toValue(value));
		else
			setProperty(entity, prop, HUtil.convertValue(value, prop.getPropertyType()));
	}

	/**
	 * Writes a bean property's value in the entity instance, converting
	 * from a byte[] to the property type
	 */
	protected void setProperty(T entity, PropertyDescriptor prop, Object value) throws HBaseException {
		if (prop.getWriteMethod() == null) {
			log.warn(String.format("Bean property %s is read-only", prop.getName()));
		}
		else {
			// narrow the type if necessary
			Object propValue = value;
			if (value != null && !prop.getPropertyType().equals(value.getClass())) {
				try {
					propValue = HUtil.cast(value, prop.getPropertyType());
				}
				catch (ClassCastException cce) {
					log.error( String.format("Unable to cast value type (%s) to type (%s)", 
							   value.getClass().getName(), prop.getPropertyType().getName()), cce );
					throw new HBaseException("Unable to populate entity", cce);
				}			
			}
			Method setter = prop.getWriteMethod();
			try {
				setter.invoke(entity, propValue);
			}
			catch (InvocationTargetException exc) {
				log.error(String.format("Error calling property setter: %s.%s", entity.getClass().getName(), setter.getName()), exc);
				throw new HBaseException("Unable to populate entity", exc);
			}
			catch (IllegalAccessException iae) {
				log.error(String.format("Error calling property setter: %s.%s", entity.getClass().getName(), setter.getName()), iae);					
				throw new HBaseException("Unable to populate entity", iae);
			}
			catch (IllegalArgumentException iae) {
				log.error(String.format("Bad argument type calling property setter: %s.%s", entity.getClass().getName(), setter.getName()), iae);					
				throw new HBaseException("Unable to populate entity", iae);
			}
		}		
	}
}
