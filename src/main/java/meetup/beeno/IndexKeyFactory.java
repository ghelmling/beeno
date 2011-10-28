package meetup.beeno;

public interface IndexKeyFactory {

	public byte[] createKey(byte[] primaryVal, byte[] rowKey, Long date, boolean invertDate);
}
