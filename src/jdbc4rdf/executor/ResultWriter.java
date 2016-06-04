package jdbc4rdf.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import jdbc4rdf.loader.io.FileAppender;



public class ResultWriter {
	
	private File resultFile;
	private FileAppender writer = new FileAppender();
	
	final static Logger logger = Logger.getLogger(ResultWriter.class);
	
	/**
	 * 
	 * Open a new file called results.txt. If a file with that name already exists, old will be deleted!
	 */
	public void newFile() {
		resultFile = new File("results.csv");
		// create the file if it does not exist yet
		if (!resultFile.exists()) {
			try {
				resultFile.createNewFile();
			} catch (IOException e) {
				logger.error("Unable to create results file", e);
			}
		}else{
			try {
				resultFile.delete();
				resultFile.createNewFile();
			} catch (IOException e) {
				logger.error("Unable to delete and re-create results file", e);
			}
		}		
	}
	
	
	/**
	 * 
	 * Every line in the ArrayList will be appended to result file. 
	 * After every line a '\n' linebreak is added.
	 * @param data ArrayList<String> 
	 */
	public void writeFile(ArrayList<String> data){
		for(int i = 0; i < data.size(); i++){
			writer.appendLine(resultFile, data.get(i)+"\n");
		}
	}
	
	
}
