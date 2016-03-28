package jdbc4rdf.loader;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public abstract class TypeDetector {

	
	private final Map<Integer, String> typeMap = new HashMap<Integer, String>();

	
	public TypeDetector() {
		// initialize type map
		for (Field field : Types.class.getFields()) {
			
			// attempt to fill this map
	        try {
				typeMap.put((Integer) field.get(null), field.getName());
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			
	    }
	}
	
	
	
	public String getTypeName(int typeIdx) {

		// look-up in the type map
		
		return this.typeMap.get(typeIdx);
	}
	
	
	
	
	
	// TODO: use JDBCType instead of int
	// This is only supported in java 1.8+
	
	/**
	 * Detect the type of the object associated with this predicate
	 * of a triple. Subject type is always string
	 * @param predIn The predicate as string
	 * @return Appropriate type out of the values in java.sql.Types
	 */
	abstract int detectObjectType(String predIn);
	
}
