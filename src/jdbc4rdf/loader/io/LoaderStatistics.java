package jdbc4rdf.loader.io;

import java.util.ArrayList;
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
	
	
	private String statsFile = "";
	
	
	Map<String, Integer> vpPredSize = null;
	
	//private ArrayList<VPStat> vpStats = new ArrayList<VPStat>();
	
	// TODO: append to file while loading
	// this data structure is only used for testing purposes
	private ArrayList<String> lines = null;
	
	
	
	public LoaderStatistics() {
		vpPredSize = new HashMap<String, Integer>();
		lines = new ArrayList<String>();
	}
	
	
	public void newFile(String statType) {
		statsFile = "stat_" + statType.toLowerCase() + ".txt";
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
		
		// TODO: create file
	}
	
	
	public void closeFile(boolean isVp) {
		if (isVp) {
			allPossibleTables = pcount;
		} else {
			allPossibleTables = pcount * pcount;
		}
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
	
	public void addVPStatistic(String pred, int vpSize) {
		String line = "<" + pred + ">";
		line += "\t" + vpSize;
		line += "\t" + size;
		line += "\t" + Helper.getRatio(vpSize, size);
		// TODO: Write to file!
		// lines.add(line);
		
		// store the information for later
		this.vpPredSize.put(pred, vpSize);
	}
	
	
	/*
	
	private class VPStatEntry {
		
		private String pred = "";
		
		private int vpSize = -1;
		
		public VPStatEntry(String pred, int vpSize) {
			this.pred = pred;
			this.vpSize = vpSize;
		}
		
		public String getPredicate() {
			return this.pred;
		}
		
		public int getVPSize() {
			return this.vpSize;
		}
		
	}
	
	private class ExtVPStatEntry extends VPStatEntry {

		
		private int extVPSize = 0;
		
		public ExtVPStatEntry(String pred, int vpSize, int extVPSize) {
			super(pred, vpSize);
		}
		
		public int getExtVPSize() {
			return this.extVPSize;
		}
		
	}
	*/
	
	
	
}
