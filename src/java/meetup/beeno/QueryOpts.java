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
	private boolean useIndex = true;
	private Criteria criteria = new Criteria();
	
	public QueryOpts() {}
	
	public QueryOpts(QueryOpts toCopy) {
		this.startKey = toCopy.startKey;
		this.pageSize = toCopy.pageSize;
		this.startTime = toCopy.startTime;
		this.useIndex = toCopy.useIndex;
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
	
	public boolean shouldUseIndex() {
		return useIndex;
	}

	public void setUseIndex( boolean useIndex ) {
		this.useIndex = useIndex;
	}

	public Long getStartTime() { return this.startTime; }
	public void setStartTime(Long time) { this.startTime = time; }
	
	public int getPageSize() { return this.pageSize; }
	public void setPageSize(int size) { this.pageSize = size; }

	public Criteria getCriteria() { return this.criteria; }
	public void setCriteria(Criteria criteria) { this.criteria = criteria; }
	
	public void addCriteria(Criteria.Expression expression) {
		this.criteria.add(expression);
	}

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
		useIndex = in.readBoolean();
		criteria = (in.readBoolean() ? null : (Criteria)in.readObject());		
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
		out.writeBoolean(this.useIndex);
		IOUtil.writeNullable(out, this.criteria);
	}

}