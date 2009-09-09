package meetup.beeno;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

@SuppressWarnings("deprecation")
public class PBRowResult{

	@SuppressWarnings("deprecation")
	public static String rrToString(RowResult rr) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=");
		sb.append(Bytes.toString(rr.getRow()));
		sb.append(", cells={");
		boolean moreThanOne = false;
		for (Map.Entry<byte[], Cell> e : rr.entrySet()) {
			if (moreThanOne) {
				sb.append(", ");
			}
			else {
				moreThanOne = true;
			}
			sb.append("(column=");
			sb.append(Bytes.toString(e.getKey()));
			sb.append(", timestamp=");
			sb.append(Long.toString(e.getValue().getTimestamp()));
			sb.append(", value=");
			byte[] v = e.getValue().getValue();
			if (Bytes.equals(e.getKey(), HConstants.REGIONINFO_QUALIFIER)) {
				try {
					sb.append(Writables.getHRegionInfo(v).toString());
				}
				catch (IOException ioe) {
					sb.append(ioe.toString());
				}
			}
			else {
				sb.append(PBUtil.toValue(v));
			}
			sb.append(")");
		}
		sb.append("}");
		return sb.toString();
	}
}
