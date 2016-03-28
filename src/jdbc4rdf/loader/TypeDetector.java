package jdbc4rdf.loader;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public abstract class TypeDetector {

	
	private final int MAX_STR_SIZE = 1024;
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
		return getTypeName(typeIdx, false);
	}


	public String getTypeName(int typeIdx, boolean stringSupported) {

		// look-up in the type map
		String typeStr = this.typeMap.get(typeIdx);

		// check if arguments are required
		if ((typeIdx == Types.VARCHAR) 
				|| (typeIdx == Types.CHAR)) {
			// if the string datatype is supported by the driver
			if (stringSupported) {
				return "string";
			} else {
				typeStr += " (" + MAX_STR_SIZE + ")";
			}
		}

		return typeStr;
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
