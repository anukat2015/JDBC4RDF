package jdbc4rdf.loader;



public class Helper {

	
	public static String cleanPredicate(String pred) {
		pred = pred.replace(":", "__");
		
		return pred;
	}
	
	
	public static String getRatio(int x1, int x2) {
		
		final String res = String.valueOf( ( ((float) x1) / ((float) x2) ) );
		
		return res;
		
	}
}
