package test.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import jdbc4rdf.executor.SQLExecutor;


public class PropertiesDemo {

	
	
	
	
	public static void main(String[] args) {

		/*
		ConfigReader cr = new ConfigReader(new String[]{"conf.txt"});
		//cr.loadConfig();

		ExecutorConfig ex = cr.getConfig(ExecutorConfig.class);
		LoaderConfig ex2 = cr.getConfig(LoaderConfig.class);
*/
		
		System.out.println("\n\n");
		
		String[] lines = new String[]{"xy=asbssd d daas", " v = ddasd s aasd22#", "asdf.qwert.yy=22s:77skkvo////", " # this is a comment", "another.one=testvalue"};

		for (String line : lines) {
			line = line.trim();
			if (!(line.isEmpty() || line.startsWith("#") )) {
				// key value pairs are separated by "="
				int idx = line.indexOf("=");
				String key = line.substring(0, idx);
				String value = line.substring(idx + 1, line.length());
				System.out.println("Found config key=\"" + key + "\", value=\"" + value + "\"");
			}
		}
		
		
		System.out.println("\n\n");
		
		
		HashMap<String, String> hm = new HashMap<String, String>();
		hm.put("key1", "val1");
		System.out.println(hm.get("key1"));
		hm.put("key1", "val2");
		System.out.println(hm.get("key1"));
		
		
		System.out.println("\n\n");
		
		//  <empty> = all, 1 = only first, 5 = first 5, -5 = last five, 4:8 query 4 - query 8
		ArrayList<String> queries = new ArrayList<String>();
		for (int i = 0; i < 9; i++) queries.add("QUERY_" + (i+1));
		
		SQLExecutor.applyFilter(queries, "0:2");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, "4");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, "1:");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, ":1");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, ":-3");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, "-3:");
		
		System.out.println();
		
		SQLExecutor.applyFilter(queries, "-5:-8");

		System.out.println();
		
		SQLExecutor.applyFilter(queries, "-8:-5");

		
	}

}
