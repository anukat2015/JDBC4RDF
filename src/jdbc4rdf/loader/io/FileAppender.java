package jdbc4rdf.loader.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class FileAppender {

	
	/*
	 * TODO: 
	 * Asynchronous write!
	 * This might be useful for avoiding load-idles due to this writing process
	 */
	
	
	public boolean appendLine(File f, String line) {
		try {
		    Files.write(f.toPath(), line.getBytes(), StandardOpenOption.APPEND);
		} catch (IOException e) {
		    e.printStackTrace();
		    return false;
		}
		
		return true;
	}
	
}
