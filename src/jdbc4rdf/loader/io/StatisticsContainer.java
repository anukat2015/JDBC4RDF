
package jdbc4rdf.loader.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class StatisticsContainer {

	public final String fileName;
	
	public final ArrayList<String> content = new ArrayList<String>();
	
	
	
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
				e.printStackTrace();
			}
		}
		
		final FileAppender writer = new FileAppender();
		for (int i = 0; i < content.size(); i++) {
			String line = content.get(i);
			writer.appendLine(statsFile, line);
		}
		
	}
	
}
