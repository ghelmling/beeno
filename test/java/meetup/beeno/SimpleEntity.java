package meetup.beeno;

import meetup.beeno.HEntity;
import meetup.beeno.HIndex;
import meetup.beeno.HProperty;
import meetup.beeno.HRowKey;

/**
 * Simple entity class with mapped properties
 */
@HEntity(name="test_simple")
public class SimpleEntity {
    String id;
    String stringProperty;
    int intProperty;
    float floatProperty;
    double doubleProperty;
    long updated = System.currentTimeMillis();
    String photoId;
    
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

    @HProperty(family="props", name="photoId", indexes = {@HIndex(date_col="props:updated", date_invert=true)})
    public String getPhotoIdProperty() { return photoId; }
    public void setPhotoIdProperty( String photoId ) { 
        this.photoId = photoId; 
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
  
    @HProperty(family="props", name="updated")
    public long getUpdated() { return updated; }
    public void setUpdated( long updateTime ) { 
        this.updated = updateTime; 
    }
}

