package test.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import jdbc4rdf.executor.SQLExecutor;
import junit.framework.TestCase;

public class QueryFilterTest extends TestCase {

	
	private static int TEST_SIZE = 10;
	
	private ArrayList<String> filterList = new ArrayList<String>();
	
	
	
	protected void setUp() throws Exception {
		
		super.setUp();
		
		// Populate the data structure which the filter will be applied on
		
		for (int i = 0; i < TEST_SIZE; i++) {
			this.filterList.add("v" + (i + 1));
		}
		
		
	}

	
	private String[] toArray(List<String> list) {
		String[] output = new String[list.size()];
		output = list.toArray(output);
		
		return output;
	}
	
	
	
	
	public void testFilter() {
		String filter;
		String[] target;
		String[] result;
		
		
		
		
		filter = "";
		target = toArray(this.filterList).clone();
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		
		
		
		filter = "3";
		target = new String[]{"v4"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "0";
		target = new String[]{"v1"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		
		
		
		filter = "-3";
		target = new String[]{"v8"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = ":3";
		target = new String[]{"v1", "v2", "v3"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = ":-3";
		target = new String[]{"v1", "v2", "v3", "v4", "v5", "v6", "v7"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "3:";
		target = new String[]{"v4", "v5", "v6", "v7", "v8", "v9", "v10"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "-3:";
		target = new String[]{"v8", "v9", "v10"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		
		
		
		filter = "1:4";
		target = new String[]{"v2", "v3", "v4"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "-1:-4";
		target = new String[]{};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "-1:4";
		target = new String[]{};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "1:-4";
		target = new String[]{"v2", "v3", "v4", "v5", "v6"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		
		
		
		filter = "4:1";
		target = new String[]{};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "-4:-1";
		target = new String[]{"v7", "v8", "v9"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "-4:1";
		target = new String[]{};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		filter = "4:-1";
		target = new String[]{"v5", "v6", "v7", "v8", "v9"};
		result = toArray(SQLExecutor.applyFilter(this.filterList, filter));
		assertTrue(Arrays.equals(target, result));
		
		
	}
	
	
	

}
