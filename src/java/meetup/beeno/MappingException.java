package meetup.beeno;

public class MappingException extends HBaseException {

	private Class entityClass = null;
	
	public MappingException( Class entityClass ) {
		this.entityClass = entityClass;
	}

	public MappingException( Class entityClass, String message ) {
		super(message);
		this.entityClass = entityClass;
	}

	public MappingException( Class entityClass, Throwable cause ) {
		super(cause);
		this.entityClass = entityClass;
	}

	public MappingException( Class entityClass, String message, Throwable cause ) {
		super(message, cause);
		this.entityClass = entityClass;
	}

	public Class getEntityClass() { return this.entityClass; }
}
