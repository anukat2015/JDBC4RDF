
package jdbc4rdf.loader.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class StatisticsContainer {

	public final String fileName;
	
	public final ArrayList<String> content = new ArrayList<String>();
	
	final static Logger logger = Logger.getLogger(StatisticsContainer.class);
	
	
	public StatisticsContainer(String f) {
		this.fileName = f;
	}
	
	
	public void addLine(String line) {
		this.content.add(line);
	}
	
	public void writeFile() {
		
		// create the file
		
		File statsFile = new File(fileName);
		
		// create the file if it does not exist yet
		if (!statsFile.exists()) {
			try {
				statsFile.createNewFile();
			} catch (IOException e) {
				logger.error("An error occured while trying to create a new statistics file", e);
			}
		}
		
		final FileAppender writer = new FileAppender();
		for (int i = 0; i < content.size(); i++) {
			String line = content.get(i);
			writer.appendLine(statsFile, line);
		}
		
	}
	
}
