package jdbc4rdf.loader;

public interface TypeDetector {

	
	/**
	 * Detect the type of the object associated with this predicate
	 * of a triple. Subject type is always string
	 * @param predIn The predicate as string
	 * @return Appropriate type out of the values in java.sql.Types
	 */
	abstract int detectObjectType(String predIn);
	
}
