package jdbc4rdf.loader;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public abstract class TypeDetector {

	/**
	 * Maximum string size for string types which require a 
	 * max-size argument (for VARCHAR() / CHAR()) <br><br>
	 * 
	 * The following values were calculated from the dictionary of
	 * the bsbm0.2 and bsbm078 DataGenerator (Both use the same dictionary)<br><br>
	 * 
	 * <pre>
	 *  Comments: 50-150<br>
	 *  
	 *  Shortest Word Length: 3
	 *  Longest Word length: 29
	 *  Average Word Length (Arithmetic Mean): 9.83
	 *  Standard Deviation: 6.18
	 *  Upper bound: 4350 characters (150*29)
	 *  Average String size: 1500 (150*10)
	 *  Lower bound: 450 (150*3)
	 *  <br>
	 *  4096 might suffice
	 *  </pre>
	 *  @see http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/Dataset/
	 *  <br>
	 *  
	 */
	public static final int MAX_STR_SIZE = 4096;
	
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
		if((typeIdx == Types.INTEGER && stringSupported)){
			return "INT";
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
