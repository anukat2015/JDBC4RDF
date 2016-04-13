package jdbc4rdf.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import jdbc4rdf.loader.io.FileAppender;

public class ResultWriter {
	
	private File resultFile;
	private FileAppender writer = new FileAppender();
	
	/**
	 * 
	 * Open a new file called results.txt. If a file with that name already exists, old will be deleted!
	 */
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
