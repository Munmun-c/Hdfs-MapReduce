package mapReduce;

import java.util.ArrayList;

public class jobData{
	public synchronized ArrayList<Integer> getTaskIds() {
		return taskIds;
	}
	public synchronized void setTaskIds(ArrayList<Integer> taskIds) {
		this.taskIds = taskIds;
	}
	public synchronized ArrayList<Boolean> getFlags() {
		return flags;
	}
	public synchronized void setFlags(ArrayList<Boolean> flags) {
		this.flags = flags;
	}
	public synchronized int getNumReducer() {
		return numReducer;
	}
	public synchronized void setNumReducer(int numReducer) {
		this.numReducer = numReducer;
	}
	public synchronized int getTaskCompleted() {
		return taskCompleted;
	}
	public synchronized void setTaskCompleted(int taskCompleted) {
		this.taskCompleted = taskCompleted;
	}
	ArrayList<Integer> taskIds;
	ArrayList<Boolean> flags;
	int numReducer;
	int taskCompleted;
	String inpFile;
	String opFile;
	String mapName;
	String reducerName;
	ArrayList<Integer> redIds;
	ArrayList<Boolean> redFlags;
	int redCompleted;
	public synchronized String getInpFile() {
		return inpFile;
	}
	public synchronized void setInpFile(String inpFile) {
		this.inpFile = inpFile;
	}
	public synchronized String getOpFile() {
		return opFile;
	}
	public synchronized void setOpFile(String opFile) {
		this.opFile = opFile;
	}
	public synchronized String getMapName() {
		return mapName;
	}
	public synchronized void setMapName(String mapName) {
		this.mapName = mapName;
	}
	public synchronized String getReducerName() {
		return reducerName;
	}
	public synchronized void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}
	public synchronized ArrayList<Integer> getRedIds() {
		return redIds;
	}
	public synchronized void setRedIds(ArrayList<Integer> redIds) {
		this.redIds = redIds;
	}
	public synchronized ArrayList<Boolean> getRedFlags() {
		return redFlags;
	}
	public synchronized void setRedFlags(ArrayList<Boolean> redFlags) {
		this.redFlags = redFlags;
	}
	public jobData() {
		// TODO Auto-generated constructor stub
		taskCompleted=0;
		redCompleted =0;
		taskIds = new ArrayList<Integer>();
		flags = new ArrayList<Boolean>();
		redFlags = new ArrayList<Boolean>();
		redIds = new ArrayList<Integer>();
	}
	public synchronized int getRedCompleted() {
		return redCompleted;
	}
	public synchronized void setRedCompleted(int redCompleted) {
		this.redCompleted = redCompleted;
	}
	
}
