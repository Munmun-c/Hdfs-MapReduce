package mapReduce;
import static mapReduce.MRConstants.HB_SLEEP;
import static mapReduce.MRConstants.JT_IP;
import static mapReduce.MRConstants.JT_NAME;
import static mapReduce.MRConstants.JT_PORT;
import static mapReduce.MRConstants.THREAD_POLL;
import static mapReduce.MRConstants.TT_NUM_MTHREADS;
import static mapReduce.MRConstants.TT_NUM_RTHREADS;
import static Common.Constants.BLOCK_SIZE;
import static mapReduce.MRConstants.*;
import static Common.Constants.*;

import IDataNode.IDataNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;

import Common.Hdfs;
import Common.Hdfs.ReadBlockRequest;
import Common.Hdfs.ReadBlockResponse;
import IDataNode.IDataNode;
import mapReduce.MapReduce.*;
public class TaskTracker /*implements Runnable*/{
	int tt_id;
	MThread mtObj[];
	RThread rtObj[];
	Thread mtThread[];
	Thread rtThread[];
	//LinkedList<MapTaskInfo> mtQueue;
	//LinkedList<ReducerTaskInfo> rtQueue;
	LinkedList<MapTaskStatus> mtStatusQ; //helps to form MapTaskStatus
	HashMap<Integer,Boolean> mStatusSent; //helps to ensure that status report for tid reached and acts as queue
	LinkedList<ReduceTaskStatus> rtStatusQ; //helps to form ReduceTaskStatus
	HashMap<Integer,Boolean> rStatusSent; //helps to ensure that status report for tid reached and acts as queue
	//helper class **********************************************************
	Helper_class ob;
	//int run_control;
	//int pseudo_tid;
	
	public TaskTracker() {
		// TODO Auto-generated constructor stub
		mtObj = new MThread[TT_NUM_MTHREADS];
		rtObj = new RThread[TT_NUM_RTHREADS];
		mtThread = new Thread[TT_NUM_MTHREADS];
		rtThread = new Thread[TT_NUM_RTHREADS];
		//above default filled to false, otherwise can use Arrayfill
	/*	mtQueue = new LinkedList<MapTaskInfo>();
		rtQueue = new LinkedList<ReducerTaskInfo>();*/
		mtStatusQ = new LinkedList<MapTaskStatus>();
		for (int i = 0; i < TT_NUM_MTHREADS; i++) {
			mtStatusQ.add(null);
			
		}
		mStatusSent = new HashMap<Integer,Boolean>();
		rStatusSent = new HashMap<Integer,Boolean>();
		rtStatusQ = new LinkedList<ReduceTaskStatus>();
		for (int i = 0; i < TT_NUM_RTHREADS; i++) {
			rtStatusQ.add(null);
			
		}
		//run_control = 1;
		//pseudo_tid = 0;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Random rand = new Random();
		while(true){
			if (rand.nextInt(20) == 0) {
				System.out.println("TT still beating");
			}
			Registry registryJT;
			
			try {
				registryJT = LocateRegistry.getRegistry(JT_IP,JT_PORT );
				IJobTracker JT_Stub = (IJobTracker) registryJT.lookup(JT_NAME);
				TaskTracker thisTT = new TaskTracker();
				thisTT.tt_id = Integer.parseInt(args[0]);
				
				thisTT.ob = new Helper_class();
				thisTT.ob.initHelper();
				
				//initialize threads
				for (int i = 0; i < TT_NUM_MTHREADS; i++) {
					thisTT.mtObj[i] = new MThread(thisTT,i);
					thisTT.mtThread[i] = new Thread(thisTT.mtObj[i]);
					thisTT.mtThread[i].start();
				}
				//thisTT.run_control = 2;
				for (int i = 0; i < TT_NUM_RTHREADS; i++) {
					thisTT.rtObj[i] = new RThread(thisTT,i);
					thisTT.rtThread[i] = new Thread(thisTT.rtObj[i]);
					thisTT.rtThread[i].start();					
				}
				System.out.println("TT server ready");
				while(true){
					try {
						Thread.sleep(HB_SLEEP);       
						System.out.println("HB polled ");
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					HeartBeatRequest.Builder hbreq = HeartBeatRequest.newBuilder();
					hbreq.setTaskTrackerId(thisTT.tt_id);
					
					int freeMThreads =0, freeRThreads=0;
					for (int i = 0; i < TT_NUM_MTHREADS; i++) {
						if(thisTT.mtObj[i].getFreeStatus() == 0 && thisTT.mtObj[i].getMtf() == null){ //will BLOCK
							if (thisTT.mtStatusQ.get(i)!= null && thisTT.mtStatusQ.get(i).getTaskCompleted() == false) {
								System.out.println("going free though false "+thisTT.mtStatusQ.get(i).getTaskId());
							}
							boolean checkBeforeAssign = false;
							if (thisTT.mtStatusQ.get(i)!= null) {
								int old_taskid = thisTT.mtStatusQ.get(i).getTaskId();
								if (thisTT.mStatusSent.get(old_taskid)) {
									checkBeforeAssign = true;
									
								}
							}
							if (thisTT.mtStatusQ.get(i)== null ||  checkBeforeAssign){
								freeMThreads++; //should check condition in hashmap ?
							}
						}
					}
					for (int i = 0; i < TT_NUM_RTHREADS; i++) {
						if (thisTT.rtObj[i].getFreeStatus() == 0 && thisTT.rtObj[i].getRtf() == null) { //will BLOCK
							boolean checkBeforeAssign = false;
							if (thisTT.rtStatusQ.get(i)!= null) {
								int old_taskid = thisTT.rtStatusQ.get(i).getTaskId();
								if (thisTT.rStatusSent.get(old_taskid)) {
									checkBeforeAssign = true;
									
								}
							}
							if (thisTT.rtStatusQ.get(i)== null ||  checkBeforeAssign){
								freeRThreads++;
							}
						}
					}
					hbreq.setNumMapSlotsFree(freeMThreads);
					hbreq.setNumReduceSlotsFree(freeRThreads);
					
					//int debugMCount=0;
					for (int i = 0; i < TT_NUM_MTHREADS; i++) {
						if (thisTT.mtStatusQ.get(i) != null) {
							if (thisTT.mtStatusQ.get(i).getTaskCompleted()) {
								System.out.println("Sending task status truee"+thisTT.mtStatusQ.get(i).getTaskId());
								thisTT.mStatusSent.put(thisTT.mtStatusQ.get(i).getTaskId(), true);
							}
							else {
								System.out.println("Sending task status falsee"+thisTT.mtStatusQ.get(i).getTaskId());

							}
							hbreq.addMapStatus(thisTT.mtStatusQ.get(i)); // assumed that operation of setting the status
																		 //the sequence of which thread does first will 
																		 //eventually not matter. But does internal java 
																		 //implementation will be a issue ?
							//System.out.println("TT adding status of a map job");
							//debugMCount++;
							
						}
					}
					//int debugRcount=0;
					for (int i = 0; i < TT_NUM_RTHREADS; i++) {
						if (thisTT.rtStatusQ.get(i) != null) {
							if (thisTT.rtStatusQ.get(i).getTaskCompleted()) {
								thisTT.rStatusSent.put(thisTT.rtStatusQ.get(i).getTaskId(), true);
							}
							hbreq.addReduceStatus(thisTT.rtStatusQ.get(i));
							//System.out.println("TT adding status of a reduce job");
						//	debugRcount++;
						}						
					}
					
					byte temp[] = JT_Stub.heartBeat(hbreq.build().toByteArray());
					HeartBeatResponse hbresp = HeartBeatResponse.parseFrom(temp);
					
					//logic to handle hb resp
					if (hbresp.getStatus() !=1) {
						System.out.println("Got improper status in heartbeat response");
					}
					/*
					 * Can reverse the true done above to handle failures
					 * */
					
					List<MapTaskInfo> mapTasks = hbresp.getMapTasksList();
					int tempCounter = 0;
					if(mapTasks.size() !=0){
						for (int i = 0; i < TT_NUM_MTHREADS; i++) {
							if (thisTT.mtObj[i].getFreeStatus() == 0 && thisTT.mtObj[i].getMtf() == null) {
								//thisTT.mtQueue.add(i, mapTasks.get(tempCounter)); // i want a deep copy here
								//am i getting it ?
								if (thisTT.mtStatusQ.get(i)!= null && thisTT.mtStatusQ.get(i).getTaskCompleted() == false) {
									System.out.println("though False, got overwritten by new");
								}
								boolean checkBeforeAssign = false;
								if (thisTT.mtStatusQ.get(i)!= null) {
									int old_taskid = thisTT.mtStatusQ.get(i).getTaskId();
									if (thisTT.mStatusSent.get(old_taskid)) {
										checkBeforeAssign = true;
										//thisTT.mStatusSent.remove(old_taskid);
									}
								}
								if (thisTT.mtStatusQ.get(i)== null ||  checkBeforeAssign){//thisTT.mtStatusQ.get(i).getTaskCompleted()) {
									System.out.println("Assigning task "+mapTasks.get(tempCounter).getTaskId()+ " to "+i);
									thisTT.mStatusSent.put(mapTasks.get(tempCounter).getTaskId(), false);
									thisTT.mtObj[i].setMtf(mapTasks.get(tempCounter));
									tempCounter++;
								}
								if (tempCounter == mapTasks.size()) {
									break; //we have more free threads than tasks assigned
								}
								//}
							}
						}
						if (tempCounter != mapTasks.size()) {
							System.out.println("All map tasks did not get assigned to threads yet!!");
						}
					}
					
					List<ReducerTaskInfo> redTasks = hbresp.getReduceTasksList();
					tempCounter = 0;
					if (redTasks.size() != 0) {
						for (int i = 0; i < TT_NUM_RTHREADS; i++) {
							if (thisTT.rtObj[i].getFreeStatus() == 0 && thisTT.rtObj[i].getRtf() == null) {
								//thisTT.rtQueue.add(i,redTasks.get(tempCounter));// i want a deep copy here
								//		am i getting it ?
								boolean checkBeforeAssign = false;
								if (thisTT.rtStatusQ.get(i)!= null) {
									int old_taskid = thisTT.rtStatusQ.get(i).getTaskId();
									if (thisTT.rStatusSent.get(old_taskid)) {
										checkBeforeAssign = true;
										//thisTT.rStatusSent.remove(old_taskid);
									}
								}
								if (thisTT.rtStatusQ.get(i)== null ||  checkBeforeAssign){
									thisTT.rStatusSent.put(redTasks.get(tempCounter).getTaskId(), false);
									thisTT.rtObj[i].setRtf(redTasks.get(tempCounter));
									tempCounter++;
								}
								if (tempCounter == redTasks.size()) {
									break; //we have more free threads than tasks assigned
								}
							}
						}
						if (tempCounter != redTasks.size()) {
							System.out.println("All reduce tasks did not get assigned to threads yet!!");
						}
					}
					
				}
			
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}

	}

}

class MThread implements Runnable{
	int freeStatus;
	MapTaskInfo mtf;
	TaskTracker parent;
	int thread_id;
	
	public MThread(TaskTracker temp, int id) {
		super();
		freeStatus = 0;
		mtf=null;
		parent = temp;
		thread_id = id;
	}
	
	
	public synchronized int getFreeStatus() {
					return freeStatus;
	}
	public synchronized void setFreeStatus(int freeStatus) {
		 	this.freeStatus = freeStatus;
	}
	public synchronized MapTaskInfo getMtf() {
		return mtf;
	}
	public synchronized void setMtf(MapTaskInfo mtf) {
		this.mtf = mtf;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				Thread.sleep(THREAD_POLL);     
				System.out.println("Thread polled id : "+thread_id);
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			MapTaskInfo receivedTask;
			if ((receivedTask = this.getMtf()) != null) {
				System.out.println("Got map task" + receivedTask.getTaskId());
				this.setFreeStatus(1);
				MapTaskStatus.Builder stat= MapTaskStatus.newBuilder();
				//do processing
				try {
					stat.setJobId(receivedTask.getJobId()).setTaskId(receivedTask.getTaskId()).setMapOutputFile("job_"+receivedTask.getJobId()+"_map_"+receivedTask.getTaskId()).setTaskCompleted(false);
					parent.mtStatusQ.set(thread_id, stat.build());
					//BIG BIG MISTAKE, first getInteger and now Add, add(thread_id, stat.build());
					System.out.println("Status set to false "+receivedTask.getTaskId()+" by "+thread_id);
					IMapper mapper = (IMapper) Class.forName(receivedTask.getMapName()).newInstance();
					
					BlockLocations blk = receivedTask.getInputBlocks(0);
					//there should be only one block per map task
					
					int jobid=receivedTask.getJobId();
					int taskid=receivedTask.getTaskId();
					
					//1.check whether either datanode location is on the same machine.
					//if not then use the first location to getFile from hdfs
					Registry DNRegistries[] = new Registry[DN_IPS.length];
					for(int i=0; i<DN_IPS.length;i++){
						DNRegistries[i] = LocateRegistry.getRegistry(DN_IPS[i],DN_PORTS[i]);
						
					}
					IDataNode temp1,temp2,temp3,temp4;
					temp1=(IDataNode) DNRegistries[0].lookup(DN_NAME[0]);
					temp2=(IDataNode) DNRegistries[1].lookup(DN_NAME[1]);
					temp3=(IDataNode) DNRegistries[2].lookup(DN_NAME[2]);
					temp4=(IDataNode) DNRegistries[3].lookup(DN_NAME[3]);
					IDataNode[] DN_Stubs = {temp1,temp2,temp3,temp4};
					
					int contact_DN = 0;
					if(TT_IPS[parent.tt_id].equals(blk.getLocations(1).getIp()) && TT_PORTS[parent.tt_id] == blk.getLocations(1).getPort()){
						contact_DN = 1;
					}
					System.out.println("Received block num "+blk.getBlockNumber());
					ReadBlockRequest RB=Hdfs.ReadBlockRequest.newBuilder().setBlockNumber(blk.getBlockNumber()).build();
					int DN_index = 0;
					for (int i = 0; i < DN_Stubs.length; i++) {
						if (DN_IPS[i].equals(blk.getLocations(contact_DN).getIp()) && DN_PORTS[i] == blk.getLocations(contact_DN).getPort()) {
							DN_index = i;
							break;
						}
					}
					
					byte[] myOp =DN_Stubs[DN_index].readBlock(RB.toByteArray());
					ReadBlockResponse wb;
			    	wb =ReadBlockResponse.parseFrom(myOp);
			    	if (wb.getStatus()!= 1) {
						//try on another DN, before aborting
			    		if (contact_DN == 0) {
							contact_DN = 1;
						}
			    		else {
							contact_DN = 0;
						}
			    		myOp = DN_Stubs[contact_DN].readBlock(RB.toByteArray());
			    		wb = ReadBlockResponse.parseFrom(myOp);
			    		if (wb.getStatus()!= 1) {
			    			System.out.println("Could not get the data block in Mapper thread");
			    			System.exit(-1);
			    		}
			    	}
			    	//get string representation of received data
			    	byte[] myBuffer = new byte[BLOCK_SIZE];
			    	for (int j = 0; j < wb.getDataList().size(); j++) {
						wb.getDataList().get(j).copyTo(myBuffer, j);
						//System.out.print(myBuffer[j]);
					}
			    	String fileData = new String(myBuffer); // maybe charset option will be required as second param
			    	
			    	
					//create temp output file
			    	Helper_class ob=new Helper_class();
			    	ob.initHelper();
			    	File file_temp = new File("job_"+jobid+"_map_"+taskid);
					 BufferedWriter output =new BufferedWriter(new FileWriter(file_temp));
					 String output_string,line;
					 
					//read input file line by line and call map function, and append output to temp file
					 BufferedReader br = new BufferedReader(new StringReader(fileData));
					 while ((line = br.readLine()) != null) {
						  output_string=mapper.map(line);
						  if (output_string != null) {
							  output.write(output_string);
					    	  output.write("\n");
						}
					}
					 
					 output.flush();
					 output.close();
					 
					//put temp output file to hdfs
					 ob.put("job_"+jobid+"_map_"+taskid);
					 
			    	
					
					
					
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (AccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this.setMtf(null);
				stat.setTaskCompleted(true);
				parent.mtStatusQ.set(thread_id, stat.build());//add(thread_id, stat.build());
				System.out.println("Status set to true "+receivedTask.getTaskId()+" by "+thread_id);
				this.setFreeStatus(0);
				
				System.out.println("Finished map task" +receivedTask.getTaskId());
			}
		}
	}
	
}



class RThread implements Runnable{
	int freeStatus;
	ReducerTaskInfo rtf;
	TaskTracker parent;
	int thread_id;
	
	public RThread(TaskTracker temp,int id) {
		super();
		freeStatus = 0;
		rtf=null;
		parent = temp;
		thread_id = id;
	}
	
	
	public synchronized int getFreeStatus() {
					return freeStatus;
	}
	public synchronized void setFreeStatus(int freeStatus) {
		 	this.freeStatus = freeStatus;
	}
	public synchronized ReducerTaskInfo getRtf() {
		return rtf;
	}
	public synchronized void setRtf(ReducerTaskInfo rtf) {
		this.rtf = rtf;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				Thread.sleep(THREAD_POLL);                 
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			ReducerTaskInfo receivedTask;
			if ((receivedTask = this.getRtf()) != null) { // i think accurate code would be to lock parents rtQueue
														  //lets see if we get errors
				System.out.println("Got reduce task "+receivedTask.getTaskId());
				this.setFreeStatus(1);
				ReduceTaskStatus.Builder stat= ReduceTaskStatus.newBuilder();
				//do processing
				try {
					stat.setJobId(receivedTask.getJobId()).setTaskId(receivedTask.getTaskId()).setTaskCompleted(false);
					parent.rtStatusQ.set(thread_id, stat.build());//add(thread_id, stat.build());
					IReducer reducer = (IReducer) Class.forName(receivedTask.getReducerName()).newInstance();
					String reducerOutput = receivedTask.getOutputFile();
					
					Helper_class ob=new Helper_class();
					ob.initHelper();
					String file;
					//create outputfile for this reducer
					int jobid=receivedTask.getJobId();
					int taskid=receivedTask.getTaskId();
					File file_out = new File(reducerOutput+"_"+jobid+"_"+taskid);
					BufferedWriter output =new BufferedWriter(new FileWriter(file_out,true));
					
					for (int i = 0; i < receivedTask.getMapOutputFilesCount(); i++) {
						//get the file from hdfs
						file=receivedTask.getMapOutputFiles(i);
						ob.get(file);
						//read each line of the mapopfile
						String line,output_string;
						BufferedReader br = new BufferedReader(new FileReader(file));
						while ((line = br.readLine()) != null) {
						      
					    	  output_string=reducer.reduce(line);
					    	//call reduce function on it, append its output ?
					    	  if (output_string != null) {
					    		  output.write(output_string);
						    	  output.write("\n");
					    	  }
					    }
					}
					output.close();
					ob.put(reducerOutput+"_"+jobid+"_"+taskid);
					//put output file to hdfs
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this.setRtf(null);
				stat.setTaskCompleted(true);
				parent.rtStatusQ.set(thread_id, stat.build());//add(thread_id, stat.build());
				this.setFreeStatus(0);
				System.out.println("Finished Reduce task "+receivedTask.getTaskId());
			}
		}
	}
	
}

