package jdbc4rdf.core;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class Helper {

	final static Logger logger = Logger.getLogger(Helper.class);
	
	
	
	
	public static String getTimestamp(long milliseconds) {
		SimpleDateFormat dformat = new SimpleDateFormat("YYYY:MM:dd:HH:mm:ss:SS");
		Date d = new Date(milliseconds);
		String dstr = dformat.format(d);
		
		return dstr;
	}
	
	
	
	public static void printTime(long nanoSeconds) {
		System.out.println("Time elapsed (h:m:s:ms)");
		logger.info("Time elapsed (h:m:s:ms)");
		
		
		System.out.println("\t" + (nanoSeconds / 1000000.0) + " milliseconds, or:");
		logger.info("\t" + (nanoSeconds / 1000000.0) + " milliseconds, or:");
		
		// 1*10^6
		long milliSec = (nanoSeconds / 1000000) % 1000;
		// sec = nano / 1*10^9
		// or sec = msec / 1000
		long s = nanoSeconds / 1000000000;
		long seconds = (s % 60);  
		long minutes = (s % 3600) / 60;
		long hours = s / 3600;
		
		String output = String.format("%02d:%02d:%02d:%02d", hours, minutes, seconds, milliSec);
		
		System.out.println("\t" + output + "\n");
		logger.info("\t" + output + "\n");
		
	}
	
	
	
	
}
