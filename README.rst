=====================
Meetup.Beeno Overview
=====================

The native Java API for HBase provides fairly low level access to your
data, representing rows essentially as maps of byte arrays.  To
simplify data access and manipulation, we've built a layer of common
client utilities in the com.meetup.base.db.hbase package.  The core of
this is a simple entity layer that maps Java classes -&gt; HBase tables
and back.  This layer borrows heavily from Hibernate and JPA, though
it's much, much more limited in scope.


Mapping Entities
================

HBase entities are simple POJOs with mapping annotations for defining
the HBase persistence information.  Mapped HBase entities do not need
to share any common parent.

Sample entity class::

    /**
     * Simple entity class with mapped properties
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


The following annotations are used to map the entity elements to an
HBase table.  The HBase table name is defined using a class-level
annotation, while the row-key and field mappings are defined as
method-level annotations on methods conforming to JavaBeans property
conventions.


``@HBaseEntity( name="*tablename*" )``
    This class-level annotation defines which HBase table is used to store
    the entity's data.  This is required.


``@HRowKey`` 
    This annotation defines the JavaBeans property used to store the
    entity record's row key.  This annotation is required for an entity
    class, and only a single @HRowKey annotation is allowed.


``@HProperty( family="*column family*", name="*column name*", type="(string|int_type|float_type|double_type|long_type)" )``
    This annotation maps a JavaBeans property to a field in the HBase
    table for the entity.  Since HBase groups fields into "column
    families", both the **family** and **name** arguments are
    required.  The trailing ":" often should in column family names in the
    HBase documentation should *not* be included in the **family**
    argument.  It will automatically be added when needed.  The **type**
    argument is only used when the JavaBeans property is a java.util.Map
    or java.util.Collection instance (more on this below).  In this case,
    the **type** argument defines what data type should be used in
    converting the underlying collection entry values.


``@HIndex( date_col="*family*:*column*", date_invert="(true|false)", extra_cols={} )``
    Declares an index table associated with this property (named as "*entitytable*-by_*property column*").


Properties as Collections


The mapping framework includes rudimentary support for mapping
collection types (java.util.Map or java.util.Collection instances) to
table fields.  Since mapping of collection types dynamically
determines the actual HBase column names used to store the values,
these mappings cannot easily be indexed with HBase's built-in
secondary index support.


Map Properties


Map-type entity properties can only be mapped to an *entire* column
family in a table.  This means that no other @HProperty annotations in
the entity class can reference fields in the same column family or you
will get an error about duplicate field mappings (the EntityService
will not know which property to associate a field value with when
retrieving an entity).

A Map property should be annotated using the special convention 

    @HProperty( family="*column family*", name="*", type="*value type*" )

The **name="*"** argument denotes that the map entries should be
round-tripped to any columns in the column family, using the column
name as the map entry key.


Collection Properties


Other collection-type entity properties can be mapped to a set of
columns in the HBase table, one column per collection entry.  A
collection property should be mapped using the annotation format

    @HProperty( family="*column family*", name="*base column name*", type="*entry value type*" )

Individual collection entry values will then be assigned specific
column names using the format
"*family*:*basename*_*entryindex*".


Services
========

Mapped entity instances can be saved or retrieved by use of a
``com.meetup.db.hbase.EntityService&lt;T&gt;`` instance or one of
it's subclasses.  This class supports a few basic operations to allow
retrieving and saving entity instances.::

    public class EntityService&lt;T&gt; {

        /**
         * Returns an entity instance for the given unique row key.  If a row 
         * for the given key does not exist, returns 'null'.
         */
        public T get( String rowkey )

        /**
         * Inserts or updates the entity instance (HBase does not distinguish 
         * between these operations) to its mapped HBase table
         */
        public void save( T entity )

        /**
         * Saves all entity instances in the list to the mapped HBase table.
         */
        public void saveAll( List&lt;T&gt; entities )

        /**
         * Deletes the row completely from the mapped HBase table.
         */
        public void delete( String rowKey )

        /**
         * Returns a Query instance for the mapped class.
         */
        public Query&lt;T&gt; query()

    }


Query API
=========

Some query examples from the feeds implementation.


Find all items related to a discussion::

    FeedItemService service = new FeedItemService(DiscussionItem.class);
    QueryOpts opts = new QueryOpts();
    opts.setPageSize(-1); // all items
    Query query = 
        service.query(DiscussionItem.class, opts)
               .add( Criteria.require( 
                            Criteria.eq("threadId", threadId) ) );
    List items = query.execute();


Find all greetings from a given member::

    FeedItemService service = new FeedItemService();
    QueryOpts opts = new QueryOpts();
    opts.setPageSize(-1); // all items
    Query query = 
        service.query(opts)
               .add( Criteria.require( 
                       Criteria.eq("memberId", memberId) ) )
               .add( Criteria.eq(“itemType”, “chapter_greeting”) );
    List items = query.execute();

