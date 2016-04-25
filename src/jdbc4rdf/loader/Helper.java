package jdbc4rdf.loader;

import jdbc4rdf.core.config.DBDRIVER;


public class Helper {

	
	
	public static String getPartName(String p, String delim) {
		
		
		
		// special characters
		p = p.replaceAll("[:]|[#]|[-]|[/]|[.]", delim);
		// brackets
		p = p.replaceAll("[<]|[>]", "");
		
		return p;
	}
	
	
	public static String cleanObject(String obj, boolean isTimestamp) {
		String result = obj;
		
		// if there might be some kind of type definition
		if (obj.contains("^^")) {
			
			/*
			final Pattern p = Pattern.compile("(?<=\").*?(?=\")");
			final Matcher matcher = p.matcher(obj);
			
			result = matcher.group(1);
			*/
			obj = obj.substring(1);
			result = obj.substring(0, obj.indexOf("\""));
			
			if (isTimestamp) {
				result = result.replace("T", " ");
			}
		}
		
		
		return result;
	}
	
	
	public static String getRatio(int x1, int x2) {
		
		final String res = String.valueOf( ( ((float) x1) / ((float) x2) ) );
		
		return res;
		
	}
}
