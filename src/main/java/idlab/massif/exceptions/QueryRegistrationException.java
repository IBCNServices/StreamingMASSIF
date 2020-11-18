package idlab.massif.exceptions;

/**
 * The QueryRegistrationException wraps all the exception that can occur during query registration.
 * Note that various types of subqueries need to be parsed and registered to their handeling implemention.
 * @author psbonte
 *
 */

public class QueryRegistrationException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1512081969345761510L;
	
	public QueryRegistrationException(String message) {
		super(message);
	}

	public QueryRegistrationException(String message, Throwable cause) {
		super(message,cause);
	}
	public QueryRegistrationException(Throwable cause) {
		super(cause);
	}
}
