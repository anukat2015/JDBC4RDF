package jdbc4rdf.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import jdbc4rdf.core.Helper;



public class TimestampTest {

	public static void main(String[] args) {
		
		// Test how long it takes to get milliseconds time (takes about 0.00xxxx milliseconds)
		long start = System.nanoTime();
		
		long ms = System.currentTimeMillis();
		
		long end = System.nanoTime();
		
		System.out.println("Runtime of getting the milliseconds time from the OS");
		Helper.printTime(end - start);


		
		
		System.out.println("Converting the following value to a timestamp");
		System.out.println("Milliseconds: " + ms);
		
		SimpleDateFormat dformat = new SimpleDateFormat("YYYY:MM:dd:HH:mm:ss:SS");
		Date d = new Date(ms);
		String dstr = dformat.format(d);
		System.out.println("Date: " + dstr);

	}

}
