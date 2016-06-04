package jdbc4rdf.loader.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.apache.log4j.Logger;

public class FileAppender {

	
	/*
	 * TODO: 
	 * Asynchronous write!
	 * This might be useful for avoiding load-idles due to this writing process
	 */
	final static Logger logger = Logger.getLogger(FileAppender.class);
	
	public boolean appendLine(File f, String line) {
		try {
		    Files.write(f.toPath(), line.getBytes(), StandardOpenOption.APPEND);
		} catch (IOException e) {
			logger.error("An error occured while trying to write file " + f.getPath(), e);
		    return false;
		}
		
		return true;
	}
	
}
