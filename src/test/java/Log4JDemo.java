package test.java;

import org.apache.log4j.Logger;

public class Log4JDemo {

	
	final static Logger logger = Logger.getLogger(Log4JDemo.class);
	
	
	public void test() {

		System.out.println("* Log4j Test Start *");
		
		System.out.println();
		
		System.out.println("> Looking for config in:");
		System.out.println(Thread.currentThread().getContextClassLoader().getResource(""));
		
		System.out.println();
		
		logger.debug("Debug message");
		logger.info("Info message");
		logger.warn("Warning message");
		logger.error("Error message");
		logger.fatal("Fatal message");
		
		System.out.println();
		
		System.out.println("* Log4j Test End *");
	}
	
	public static void main(String[] args) {
		Log4JDemo l4j = new Log4JDemo();
		l4j.test();
	}

}
