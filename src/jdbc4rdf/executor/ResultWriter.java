package jdbc4rdf.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.core.appender.AppenderLoggingException;

import jdbc4rdf.loader.io.FileAppender;

public class ResultWriter {
	
	private File resultFile;
	private FileAppender writer = new FileAppender();
	
	
	public void newFile() {
		resultFile = new File("results.txt");
		// create the file if it does not exist yet
		if (!resultFile.exists()) {
			try {
				resultFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			try {
				resultFile.delete();
				resultFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}
	
	public void writeFile(ArrayList<String> data){
		for(int i = 0; i < data.size(); i++){
			writer.appendLine(resultFile, data.get(i));
		}
	}
	
	
}
