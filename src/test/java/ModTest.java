package test.java;

import jdbc4rdf.executor.SQLExecutor;
import junit.framework.TestCase;

public class ModTest extends TestCase {

	private int val = 0;
	
	private int modBase = 0;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		val = -13;
		modBase = 10;
	}

	
	public void testMod() {
		int javaMod = val % modBase;
		assertEquals(-3, javaMod);
		
		int myMod = SQLExecutor.altmod(val, modBase);
		assertEquals(7, myMod);
		
	}
	
}
