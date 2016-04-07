package jdbc4rdf.loader.io;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jdbc4rdf.loader.Helper;

public class LoaderStatistics {

	
	/**
	 * Amount of triples stored in the triples table
	 */
	private int size = 0;
	
	/**
	 * Amount of (distinct) predicates in the dataset
	 */
	private int pcount = 0;
	
	
	private int savedTables = 0;
	
	private int unsavedNonEmptyTables = 0;
	
	private int allPossibleTables = 0;
	
	private File statsFile;
	
	private FileAppender writer = new FileAppender();
	
	/**
	 * Stores the amount of rows per predicate-vp table
	 */
	private Map<String, Integer> vpPredSize = null;
	
	
	public LoaderStatistics() {
		vpPredSize = new HashMap<String, Integer>();
	}
	
	
	public void newFile(String statType) {
		statsFile = new File("stat_" + statType.toLowerCase() + ".txt");
		
		// create the file if it does not exist yet
		if (!statsFile.exists()) {
			try {
				statsFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
		savedTables = 0;
		unsavedNonEmptyTables = 0;
		/*
		if (statType.equals("VP")) {
			allPossibleTables = pcount;
		} else {
			allPossibleTables = pcount * pcount;
		}
		moved to finish, because only then, the pcount is known
		*/
	}
	
	
	public void closeFile(boolean isVp) {
		if (isVp) {
			allPossibleTables = pcount;
		} else {
			allPossibleTables = pcount * pcount;
		}
		
		final int emptyTables = allPossibleTables - savedTables - unsavedNonEmptyTables;
		
		// write last view lines to file
		writer.appendLine(statsFile, "---------------------------------------------------------\n");
		writer.appendLine(statsFile, "Saved tabels ->" + savedTables + "\n");
		writer.appendLine(statsFile, "Unsaved non-empty tables ->" + unsavedNonEmptyTables + "\n");
		writer.appendLine(statsFile, "Empty tables ->" + emptyTables + "\n");
	}
	
	
	public void setDatasetSize(int size) {
		this.size = size;
	}
	
	public void setPredicateCount(int pcount) {
		this.pcount = pcount;
	}
	
	
	public void incSavedTables() {
		this.savedTables++;
	}
	
	public void incUnsavedNonEmptyTables() {
		this.unsavedNonEmptyTables++;
	}
	
	
	public void addVPStatistic(String pred, int vpSize) {
		String line = "<" + pred + ">";
		
		line += "\t" + vpSize;
		line += "\t" + size;
		
		line += "\t" + Helper.getRatio(vpSize, size);
		
		// Write to file!
		writer.appendLine(statsFile, line + "\n");
		
		// store the information for later
		this.vpPredSize.put(pred, vpSize);
	}
	
	
	public void addExtVPStatistic(String pred1, String pred2, int extVPSize) {
		
		final int sizeVp = vpPredSize.get(pred1); 
		
		String line = "<" + pred1 + "><" + pred2 + ">";
		
		line += "\t" + extVPSize;
		line += "\t" + sizeVp;
		
		line += "\t" + Helper.getRatio(extVPSize, sizeVp);
		line += "\t" + Helper.getRatio(sizeVp, size);

		
		writer.appendLine(statsFile, line + "\n");
	}
	

	
	public int getVPTableSize(String pred) {
		if (vpPredSize.containsKey(pred)) {
			return vpPredSize.get(pred);
		} else {
			return 0;
		}
	}

	
	
}
