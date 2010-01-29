/**
 * 
 */
package meetup.beeno;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import meetup.beeno.util.IOUtil;

import org.apache.hadoop.hbase.util.Bytes;

public class QueryOpts implements Externalizable {
	public static final int DEFAULT_PAGE_SIZE = 50;

	private byte[] startKey = null;
	private Long startTime = null;
	private int pageSize = DEFAULT_PAGE_SIZE;
	
	public QueryOpts() {}
	
	public QueryOpts(QueryOpts toCopy) {
		this.startKey = toCopy.startKey;
		this.pageSize = toCopy.pageSize;
		this.startTime = toCopy.startTime;
	}
	
	public byte[] getStartKey() { return this.startKey; }
	public void setStartKey(String key) {
		if (key != null)
			this.startKey = Bytes.toBytes(key);
		else
			this.startKey = null;
	}
	
	public void setStartKey(byte[] key) {
		this.startKey = key;
	}
	
	public Long getStartTime() { return this.startTime; }
	public void setStartTime(Long time) { this.startTime = time; }
	
	public int getPageSize() { return this.pageSize; }
	public void setPageSize(int size) { this.pageSize = size; }

	@Override
	public void readExternal( ObjectInput in ) throws IOException,
			ClassNotFoundException {

		if (in.readBoolean())
			startKey = null;
		else {
			int numbytes = in.readInt();
			startKey = new byte[numbytes];
			in.readFully(startKey);
		}
		startTime = IOUtil.readLong(in);
		pageSize = in.readInt();
	}

	@Override
	public void writeExternal( ObjectOutput out ) throws IOException {
		out.writeBoolean( this.startKey == null );
		if (this.startKey != null) {
			out.writeInt(this.startKey.length);
			out.write(this.startKey);
		}
		IOUtil.writeNullable(out, this.startTime);
		out.writeInt(this.pageSize);
	}

}