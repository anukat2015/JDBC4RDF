package jdbc4rdf.loader;

import java.sql.Types;
import java.util.ArrayList;

public class BSBMTypeDetector extends TypeDetector {

	
	private ArrayList<String> DATETIME_TYPES;
	
	private ArrayList<String> DATE_TYPES;
	
	private ArrayList<String> DOUBLE_TYPES;
	
	private ArrayList<String> INTEGER_TYPES;
	
	
	public BSBMTypeDetector() {
		super();
		
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
		this.DATETIME_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewDate>");
		this.DATETIME_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validFrom>");
		this.DATETIME_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo>");
		
		this.DATETIME_TYPES = getShortPredicates(DATETIME_TYPES);
		
		
		/*
		 * Initialize date types
		 * http://purl.org/dc/elements/1.1/date
		 */
		this.DATE_TYPES = new ArrayList<String>();
		this.DATE_TYPES.add("http://purl.org/dc/elements/1.1/date");
		this.DATE_TYPES.add("<http://purl.org/dc/elements/1.1/date>");
		
		this.DATE_TYPES = getShortPredicates(DATE_TYPES);
		
		/*
		 * Initialize double types
		 */
		this.DOUBLE_TYPES = new ArrayList<String>();
		this.DOUBLE_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price");
		this.DOUBLE_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price>");
		this.DOUBLE_TYPES.add("url/vocabulary/price");
		
		this.DOUBLE_TYPES = getShortPredicates(DOUBLE_TYPES);
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
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4'
		 * http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating5"
		 */
		this.INTEGER_TYPES = new ArrayList<String>();
		this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays");
		this.INTEGER_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays>");
		// add numeric properties from 1 - 6
		for (int i = 1; i < 7; i++) {
			this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric" + i);
			this.INTEGER_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric" + i +">");
		}
		// add ratings from 1 - 5
		for (int i = 1; i < 6; i++) {
			this.INTEGER_TYPES.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating" + i);
			this.INTEGER_TYPES.add("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating" + i + ">");
		}
		
		this.INTEGER_TYPES = getShortPredicates(INTEGER_TYPES);
		
	}
	
	
	@Override
	public int detectObjectType(String predIn) {
		String pred = predIn;
		
		// remove brackets and special characters
		if(pred.startsWith("<") && pred.endsWith(">")) {
			pred = pred.substring(1, pred.length() - 1);
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
		} else{
			// default type: string
			return Types.VARCHAR;
		}

		
	}
	
	public ArrayList<String> getShortPredicates(ArrayList<String> preds){
		
		ArrayList<String> shortPreds = new ArrayList<String>();
		
		
		for(int i = 0; i < preds.size(); i++){
			String id = preds.get(i);
			id = id.substring(id.length() - 20);
			shortPreds.add(id);
		}
		
		preds.addAll(shortPreds);
		return (ArrayList<String>) preds.clone();
		
	}


	
}
