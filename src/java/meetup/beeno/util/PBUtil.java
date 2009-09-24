/**
 * 
 */
package meetup.beeno.util;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;

import meetup.beeno.HDataTypes;
import meetup.beeno.HDataTypes.DateTime;
import meetup.beeno.HDataTypes.HField;
import meetup.beeno.HDataTypes.JavaEnum;
import meetup.beeno.HDataTypes.StringList;
import meetup.beeno.HDataTypes.HField.Type;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Assorted utility methods for working with Google Protocol Buffers
 * 
 * @author garyh
 *
 */
public class PBUtil {

	private static Logger log = Logger.getLogger(PBUtil.class);
	
	
	/* ********** Google Protocol Buffer versions for serialization *********** */
	public static byte[] toBytes(Object val) {
		if (val == null)
			return null;

		Message pb = null;
		if (val.getClass().isArray() && val.getClass().getComponentType() == Byte.TYPE) {
			pb = toMessage( (byte[])val );
		}		
		else if (val instanceof Integer) {
			pb = toMessage( (Integer)val );
		}
		else if (val instanceof Float) {
			pb = toMessage( (Float)val );
		}
		else if (val instanceof Double) {
			pb = toMessage( (Double)val );
		}
		else if (val instanceof Long) {
			pb = toMessage( (Long)val );
		}
		else if (val instanceof String) {
			pb = toMessage( (String)val );
		}
		else if (val instanceof Date) {
			pb = toMessage( (Date)val );
		}
		else if (val instanceof Enum) {
			pb = toMessage( (Enum)val );
		}
		else if (val instanceof Collection) {
			// assume it's a string collection for now!
			pb = toMessage( (Collection)val );
		}
		else {
			// not handled
			log.warn(String.format("Unknown conversion to protobuf for property value type %s", val.getClass().getName()));
			return null;
		}
		
		if (pb != null) {
			// self describing message
			return pb.toByteArray();
		}
		
		return null;
	}
	
	public static HDataTypes.HField readMessage(byte[] bytes) {
		if (bytes == null || bytes.length == 0)
			return null;
		
		// convert to the underlying message type
		HDataTypes.HField field = null;
		try {
			field = HDataTypes.HField.parseFrom(bytes);
			if (log.isDebugEnabled())
				log.debug("Read field:\n"+field.toString());
		}
		catch (InvalidProtocolBufferException e) {
			log.error("Invalid protocol buffer parsing bytes", e);
		}
		
		return field;
	}
	
	public static Object toValue(byte[] bytes) {
		HDataTypes.HField field = readMessage(bytes);
		if (field == null)
			return null;
		
		switch (field.getType()) {
		case TEXT:
			return field.getText();
		case INTEGER:
			return field.getInteger();
		case FLOAT:
			return field.getFloat();
		case BOOLEAN:
			return field.getBoolean();
		case BINARY:
			return field.getBinary().toByteArray();
		case DATETIME:
			HDataTypes.DateTime dt = field.getDateTime();
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(dt.getTimestamp());
			if (dt.hasTimezone())
				cal.setTimeZone( TimeZone.getTimeZone(dt.getTimezone()) );
			return cal.getTime();
		case JAVAENUM:
			try {
				HDataTypes.JavaEnum en = field.getJavaEnum();
				Class enumClass = Class.forName(en.getType());
				return Enum.valueOf(enumClass, en.getValue());
			}
			catch (Exception e) {
				log.error("Error instantiating field value for JavaEnum", e);
			}
			break;
		case STRINGLIST:
			return field.getStringList().getValuesList();
		default:
			log.error("Unknown field type "+field.getType());
		}
		
		return null;
	}
	
	public static HDataTypes.HField toMessage(String val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.TEXT)
			.setText(val)
			.build();
	}
	
	public static HDataTypes.HField toMessage(Integer val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.INTEGER)
			.setInteger(val)
			.build();
	}

	public static HDataTypes.HField toMessage(Long val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.INTEGER)
			.setInteger(val)
			.build();
	}

	public static HDataTypes.HField toMessage(Float val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.FLOAT)
			.setFloat(val)
			.build();
	}

	public static HDataTypes.HField toMessage(Double val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.FLOAT)
			.setFloat(val)
			.build();
	}
	
	public static HDataTypes.HField toMessage(Date val) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(val);
		HDataTypes.DateTime dt = HDataTypes.DateTime.newBuilder()
			.setTimestamp(cal.getTime().getTime())
			.setTimezone(cal.getTimeZone().getID())
			.build();
		
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.DATETIME)
			.setDateTime(dt)
			.build();
	}
	
	public static HDataTypes.HField toMessage(Boolean val) {
		return HDataTypes.HField.newBuilder()
			.setType(HDataTypes.HField.Type.BOOLEAN)
			.setBoolean(val)
			.build();
	}
	
	public static HDataTypes.HField toMessage(byte[] val) {
		ByteString bytes = ByteString.copyFrom(val);
		
		return HDataTypes.HField.newBuilder()
			.setType( HDataTypes.HField.Type.BINARY )
			.setBinary( bytes )
			.build();
	}
	
	public static HDataTypes.HField toMessage(Enum val) {
		HDataTypes.JavaEnum en = HDataTypes.JavaEnum.newBuilder()
			.setType( val.getClass().getName() )
			.setValue( val.name() )
			.build();
		
		return HDataTypes.HField.newBuilder()
			.setType( HDataTypes.HField.Type.JAVAENUM )
			.setJavaEnum( en )
			.build();
	}
	
	public static HDataTypes.HField toMessage(Collection<? extends String> vals) {
		HDataTypes.StringList list = HDataTypes.StringList.newBuilder()
			.addAllValues(vals)
			.build();
		
		return HDataTypes.HField.newBuilder()
			.setType( HDataTypes.HField.Type.STRINGLIST )
			.setStringList( list )
			.build();
	}
}
