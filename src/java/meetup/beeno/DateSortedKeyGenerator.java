package meetup.beeno;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class DateSortedKeyGenerator {
	private byte[] sep = new byte[]{ '-' };
	private byte[][] columns = null;
	private byte[] datecol = null;
	private boolean invertdate = false;

	public DateSortedKeyGenerator(byte[][] indexcols, byte[] datecol, boolean descending) {
		this.columns = indexcols;
		this.datecol = datecol;
		this.invertdate = descending;
	}

	public DateSortedKeyGenerator() {
		// For Writeable
	}

	/** {@inheritDoc} */
	public byte[] createIndexKey(byte[] rowKey, Map<byte[], byte[]> rowcolumns) {
		byte[] key = new byte[0];
		for (byte[] col : this.columns) {
			key = Bytes.add(key, rowcolumns.get(col));
		}
		if (rowcolumns.get(this.datecol) != null) {
			// datecol must contain a Long value!!!
			key = Bytes.add(key, 
					HUtil.toOrderedBytes( PBUtil.readMessage(rowcolumns.get(this.datecol)).getInteger(), this.invertdate ));
		}
		return key;
	}
	
	/** {@inheritDoc} */
	public void readFields(DataInput in) throws IOException {
		int collength = WritableUtils.readVInt(in);
		this.columns = new byte[collength][];
		for (int i=0; i<collength; i++) {
			this.columns[i] = Bytes.readByteArray(in);
		}
		this.datecol = Bytes.readByteArray(in);
		int invertint = Bytes.toInt(Bytes.readByteArray(in));
		this.invertdate = (invertint == 1);
	}

	/** {@inheritDoc} */
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, this.columns.length);
		for (byte[] col : this.columns)
			Bytes.writeByteArray(out, col);

		Bytes.writeByteArray(out, this.datecol);
		Bytes.writeByteArray(out, Bytes.toBytes( (this.invertdate ? 1 : 0) ));
	}

}