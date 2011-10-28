/**
 * 
 */
package meetup.beeno.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * Support methods for implementing {@link java.io.Externalizable} interface.
 * 
 * @author garyh
 *
 */
public class IOUtil {
	public static enum VALUE_TYPE {STRING, INT, LONG, FLOAT, DOUBLE, DATE, ENUM, OBJECT};
	private static Logger log = Logger.getLogger(IOUtil.class);

	public static void writeNullable( ObjectOutput out, Object value ) throws IOException {
		if (value == null) {
			out.writeBoolean(true);
		}
		else {
			out.writeBoolean(false);
			if (value instanceof String)
				out.writeUTF((String)value);
			else if (value instanceof Integer)
				out.writeInt(((Integer)value).intValue());
			else if (value instanceof Float)
				out.writeFloat(((Float)value).floatValue());
			else if (value instanceof Double)
				out.writeDouble(((Double)value).doubleValue());
			else if (value instanceof Long)
				out.writeLong(((Long)value).longValue());
			else if (value instanceof Date)
				out.writeLong(((Date)value).getTime());
			else if (value instanceof Enum)
				out.writeUTF(((Enum)value).name());
			else
				out.writeObject(value);
		}
	}
	
	public static void writeNullableWithType( ObjectOutput out, Object value ) throws IOException {
		if (value == null) {
			out.writeBoolean(true);
		}
		else {
			out.writeBoolean(false);
			if (value instanceof String) {
				out.writeUTF(VALUE_TYPE.STRING.name());
				out.writeUTF((String)value);
			}
			else if (value instanceof Integer) {
				out.writeUTF(VALUE_TYPE.INT.name());				
				out.writeInt(((Integer)value).intValue());
			}
			else if (value instanceof Float) {
				out.writeUTF(VALUE_TYPE.FLOAT.name());
				out.writeFloat(((Float)value).floatValue());
			}
			else if (value instanceof Double) {
				out.writeUTF(VALUE_TYPE.DOUBLE.name());
				out.writeDouble(((Double)value).doubleValue());
			}
			else if (value instanceof Long) {
				out.writeUTF(VALUE_TYPE.LONG.name());
				out.writeLong(((Long)value).longValue());
			} 
			else if (value instanceof Date) {
				out.writeUTF(VALUE_TYPE.DATE.name());
				out.writeLong(((Date)value).getTime());
			} 
			else if (value instanceof Enum) {
				out.writeUTF(VALUE_TYPE.ENUM.name());
				out.writeUTF(value.getClass().getName());
				out.writeUTF(((Enum)value).name());
			}
			else {
				out.writeUTF(VALUE_TYPE.OBJECT.name());
				out.writeObject(value);
			}
		}
	}

	public static Integer readInteger( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : new Integer(in.readInt()));
	}

	public static Long readLong( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : new Long(in.readLong()));
	}

	public static Float readFloat( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : new Float(in.readFloat()));
	}

	public static Double readDouble( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : new Double(in.readDouble()));
	}

	public static String readString( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : in.readUTF());
	}

	public static Date readDate( ObjectInput in ) throws IOException {
		return (in.readBoolean() ? null : new Date(in.readLong()));
	}

	public static <T extends Enum<T>> T readEnum( ObjectInput in, Class<T> enumType ) throws IOException {
		String enumName = readString(in);
		if (enumName == null)
			return null;
		
		return Enum.valueOf(enumType, enumName);
	}

	/**
	 * Reads back a generic object (if not null) with no guarantee as to the 
	 * returned type.
	 */
	public static Object readObject( ObjectInput in ) throws IOException, ClassNotFoundException {
		if (in.readBoolean())
			return null;

		return in.readObject();
	}


	/**
	 * Reads an object from the input stream that has been serialized by
	 * calling {@link #writeNullableWithType(ObjectOutput, Object)}.  
	 * 
	 * Note: If called on a serialized object value that has not been written
	 * with the type, this will fail!
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static Object readWithType( ObjectInput in ) throws IOException, ClassNotFoundException {
		if (in.readBoolean())
			return null;
		
		try {
			VALUE_TYPE type = VALUE_TYPE.valueOf( in.readUTF() );
			switch (type) {
			case STRING:
				return in.readUTF();
			case INT:
				return in.readInt();
			case LONG:
				return in.readLong();
			case FLOAT:
				return in.readFloat();
			case DOUBLE:
				return in.readDouble();
			case DATE:
				return new Date(in.readLong());
			case ENUM:
				try {
					Class enumClass = Class.forName(in.readUTF());
					return Enum.valueOf(enumClass, in.readUTF());
				}
				catch (Exception e) {
					log.error("Error reading enum value from object input", e);
					return null;
				}
			case OBJECT:
				return in.readObject();
			default:
				log.warn("Invalid serialized value type!");
			}
		}
		catch (IllegalArgumentException iae) {
			log.error("Invalid serialized type reading from object input stream", iae);
		}
		
		return null;
	}
}
