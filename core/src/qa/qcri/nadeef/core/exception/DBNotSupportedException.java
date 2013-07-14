package qa.qcri.nadeef.core.exception;

public class DBNotSupportedException extends Exception {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Exception innerException;

    public DBNotSupportedException(String message) {
        super(message);
    }

    public DBNotSupportedException(Exception innerException) {
        this.innerException = innerException;
    }

    @Override
    public String getMessage() {
        return this.innerException.getMessage();
    }
}
