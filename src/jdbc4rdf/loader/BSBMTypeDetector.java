package jdbc4rdf.loader;

import java.sql.Types;
import java.util.ArrayList;

public class BSBMTypeDetector extends TypeDetector {

	
	private final ArrayList<String> DATETIME_TYPES;
	
	private final ArrayList<String> DATE_TYPES;
	
	private final ArrayList<String> DOUBLE_TYPES;
	
	private final ArrayList<String> INTEGER_TYPES;
	
	
	public BSBMTypeDetector() {
		
		/*
		 * Initialize datetime types
		 * "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewDate", 
		 * "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validFrom", 
		 * "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo"
		 */
		this.DATETIME_TYPES = new ArrayList<String>();
		this.DATETIME_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewDate");
		this.DATETIME_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validFrom");
		this.DATETIME_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo");
		
		
		/*
		 * Initialize date types
		 * http://purl.org/dc/elements/1.1/date
		 */
		this.DATE_TYPES = new ArrayList<String>();
		this.DATE_TYPES.add("http://purl.org/dc/elements/1.1/date");
		
		
		/*
		 * Initialize double types
		 */
		this.DOUBLE_TYPES = new ArrayList<String>();
		this.DOUBLE_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price");
		
		
		/*
		 * Initialize integer types
		 * "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric3, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric4, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric5, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric6, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3, 
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4"
		 */
		this.INTEGER_TYPES = new ArrayList<String>();
		this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays");
		// add numeric properties from 1 - 6
		for (int i = 1; i < 7; i++) {
			this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric" + i);
		}
		// add ratings from 1 - 4
		for (int i = 1; i < 5; i++) {
			this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating" + i);
		}
		
	}
	
	
	@Override
	public int detectObjectType(String predIn) {
		String pred = predIn;
		
		// remove brackets
		if(pred.startsWith("<") && pred.endsWith(">")) {
			pred = pred.substring(1, pred.length() - 2);
		}
		
		// check type
		if (INTEGER_TYPES.contains(pred)) {
			return Types.INTEGER;
		} else if (DOUBLE_TYPES.contains(pred)) {
			return Types.DOUBLE;
		} else if (DATE_TYPES.contains(pred)) {
			return Types.DATE;
		} else if (DATETIME_TYPES.contains(pred)) {
			return Types.TIMESTAMP;
		}

		// default type: string
		return Types.VARCHAR;
	}


	
}
