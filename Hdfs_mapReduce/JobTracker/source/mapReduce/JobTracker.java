package mapReduce;

import static Common.Constants.NN_IP;
import static Common.Constants.NN_NAME;
import static Common.Constants.NN_PORT;
import static mapReduce.MRConstants.GREP_STRING_FILE;
import static mapReduce.MRConstants.JT_IP;
import static mapReduce.MRConstants.JT_NAME;
import static mapReduce.MRConstants.JT_PORT;
import static mapReduce.MRConstants.JOB_COUNTER_FILE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.MatchResult;

import com.google.protobuf.InvalidProtocolBufferException;

import Common.Hdfs;
import Common.Hdfs.BlockLocationRequest;
import Common.Hdfs.BlockLocationResponse;
import Common.Hdfs.CloseFileRequest;
import Common.Hdfs.CloseFileResponse;
import Common.Hdfs.OpenFileRequest;
import Common.Hdfs.OpenFileResponse;
import INameNode.INameNode;
import mapReduce.MapReduce.*;

//Should use setter/getter at each place ?


public class JobTracker implements IJobTracker{
	Integer jobCounter;
	Integer taskCounter;
	INameNode NN_Stub;
	LinkedList<MapTaskInfo> mQueue;
	LinkedList<ReducerTaskInfo> rQueue;
	HashMap<Integer, jobData> repo;
	
	public JobTracker() {
		// TODO Auto-generated constructor stub
		jobCounter = 0;
		taskCounter = 0;
		mQueue = new LinkedList<MapTaskInfo>();
		rQueue = new LinkedList<ReducerTaskInfo>();
		repo = new HashMap<Integer,jobData>();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.setProperty("java.rmi.server.hostname",JT_IP);
			JobTracker singleJT = new JobTracker();
			
			IJobTracker JT_stub = (IJobTracker)UnicastRemoteObject.exportObject(singleJT, JT_PORT);
			Registry registry = LocateRegistry.createRegistry(JT_PORT);
			registry.rebind(JT_NAME, JT_stub);
			
			//read from config file and initialize jobCounter (and taskCounter also ??)
			//simulating effects of periodic storage of JobTracker ids
			BufferedReader br;
			// TODO Auto-generated constructor stub
			try {
				br = new BufferedReader(new FileReader(JOB_COUNTER_FILE));
				try {
					singleJT.jobCounter = Integer.parseInt(br.readLine());
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				br.close();
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			BufferedWriter output =new BufferedWriter(new FileWriter(new File(JOB_COUNTER_FILE)));
			output.write(Integer.toString(singleJT.jobCounter+1000));
			
			output.flush();
			output.close();
			//connect to NN
			Registry registryNN = LocateRegistry.getRegistry(NN_IP,NN_PORT );	
			singleJT.NN_Stub = (INameNode) registryNN.lookup(NN_NAME);
			
			
			
			
			System.out.println("JT server ready");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println("Some error in Rmi");
		}
		
	}
	
	public MapReduce.BlockLocations translator(Common.Hdfs.BlockLocations ipBlk){
		MapReduce.BlockLocations.Builder ret = MapReduce.BlockLocations.newBuilder();
		ret.setBlockNumber(ipBlk.getBlockNumber());
		for (int i = 0; i < ipBlk.getLocationsCount(); i++) {
			Common.Hdfs.DataNodeLocation ipLocation = ipBlk.getLocations(i);
			MapReduce.DataNodeLocation.Builder temp = MapReduce.DataNodeLocation.newBuilder();
			temp.setIp(ipLocation.getIp());
			temp.setPort(ipLocation.getPort());
			ret.addLocations(temp.build());
		}
		return ret.build();
	}

	@Override
	public byte[] jobSubmit(byte[] jobReq) throws RemoteException {
		// TODO Auto-generated method stub
		JobSubmitResponse.Builder jbRes = JobSubmitResponse.newBuilder() ;
		jbRes.setStatus(0);
		try {
			JobSubmitRequest jbReq = JobSubmitRequest.parseFrom(jobReq);
			
			//copy starts
			OpenFileRequest or_get=Hdfs.OpenFileRequest.newBuilder().setFileName(jbReq.getInputFile()).setForRead(true).build();
			byte[] inp_=or_get.toByteArray();
			byte[] op_= NN_Stub.openFile(inp_);
			OpenFileResponse or2_=OpenFileResponse.parseFrom(op_);
			if (or2_.getStatus() != 1) {
				System.out.println("Error in file open - status = " + or2_.getStatus());
			}
			else {
		    	int handle_=or2_.getHandle();
		    	BlockLocationRequest.Builder Br=Hdfs.BlockLocationRequest.newBuilder();
		    	
		    	Br.addAllBlockNums(or2_.getBlockNumsList());
		    	inp_=Br.build().toByteArray();
	    		op_=NN_Stub.getBlockLocations(inp_);
	    		BlockLocationResponse Blr=BlockLocationResponse.parseFrom(op_);
	    		if (Blr.getStatus() != 1) {
	    			System.out.println("Error in BlockLocation request - status "+Blr.getStatus());
	    			//Restart of Namenode required for being able to put same filenamme again
				}
	    		//close file
		    	CloseFileRequest cf_=Hdfs.CloseFileRequest.newBuilder().setHandle(handle_).build();
		    	inp_=cf_.toByteArray(); 
		    	op_=NN_Stub.closeFile(inp_);
		    	CloseFileResponse cfr_=CloseFileResponse.parseFrom(op_);
		    	int st_cfr=cfr_.getStatus();
		    	if(st_cfr != 1){
		    		System.out.println("Error in close file request, status - "+ st_cfr);
		    	}
	    		
	    		
	    		//received block response, now allocate job id
	    		jobData addJob = new jobData();
	    		addJob.setNumReducer(jbReq.getNumReduceTasks());
	    		addJob.setTaskCompleted(0);
	    		addJob.setInpFile(jbReq.getInputFile());
	    		addJob.setOpFile(jbReq.getOutputFile());
	    		addJob.setReducerName(jbReq.getReducerName());
	    		addJob.setMapName(jbReq.getMapName());
	    		
	    		int thisJob;
	    		synchronized (jobCounter) {
	    			jobCounter++;
		    		thisJob = jobCounter;
				}
	    		repo.put(thisJob, addJob );
	    		
	    		for (int i = 0; i < Blr.getBlockLocationsCount(); i++) {
	    			synchronized (taskCounter) {
	    				taskCounter++;
	    				addJob.getTaskIds().add(taskCounter);
					}
	    			addJob.getFlags().add(false);
	    		}
	    		
	    		//job added to repo but now time to add to Queue
	    		for (int i = 0; i < Blr.getBlockLocationsCount(); i++) {
					MapTaskInfo.Builder tempMapTaskInfo = MapTaskInfo.newBuilder();
					tempMapTaskInfo.setJobId(thisJob);
					System.out.println("Got block number " + Blr.getBlockLocations(i).getBlockNumber());
					tempMapTaskInfo.setMapName(addJob.getMapName());
					tempMapTaskInfo.setTaskId(addJob.getTaskIds().get(i));
					System.out.println("Set block number " + translator(Blr.getBlockLocations(i)));
					tempMapTaskInfo.addInputBlocks(translator(Blr.getBlockLocations(i)));
					synchronized (mQueue) {
						mQueue.add(tempMapTaskInfo.build());
					}
				}
	    		
	    		//job added to fifo queue, now return the status
	    		jbRes.setJobId(thisJob);
	    		jbRes.setStatus(1);
	    		
			}

			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jbRes.build().toByteArray();
	}

	@Override
	public byte[] getJobStatus(byte[] statusReq)  throws RemoteException{
		//Assume periodic removal of stale jobids entries from Repo
		
		// TODO Auto-generated method stub
		//extract job id and just read data from jobData
		JobStatusRequest jbSReq= null;
		JobStatusResponse.Builder jbSRes = JobStatusResponse.newBuilder();
		try {
			jbSReq = JobStatusRequest.parseFrom(statusReq);
		
		jobData jbData = repo.get(jbSReq.getJobId());
		
		jbSRes.setStatus(1);
		//unfortunately implemented track task completed instead of started, could hardcode some minimum value to be sent
		jbSRes.setNumMapTasksStarted(jbData.getTaskCompleted());
		jbSRes.setTotalMapTasks(jbData.getTaskIds().size());
		jbSRes.setNumReduceTasksStarted(jbData.getRedCompleted());
		jbSRes.setTotalReduceTasks(jbData.getNumReducer());
		if(jbData.getRedCompleted() == jbData.getNumReducer()){
			jbSRes.setJobDone(true);
		}
		else {
			jbSRes.setJobDone(false);
		}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jbSRes.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] beatReq) throws RemoteException{
		// TODO Auto-generated method stub
		
		HeartBeatResponse.Builder hbRes = HeartBeatResponse.newBuilder();
		hbRes.setStatus(0);
		try {
			HeartBeatRequest hbReq = HeartBeatRequest.parseFrom(beatReq);	
		
		//process heart beat
		int TT_id = hbReq.getTaskTrackerId();
		int scheduleMapCount = hbReq.getNumMapSlotsFree();
		int scheduleReducerCount = hbReq.getNumReduceSlotsFree();
		 
		synchronized(mQueue){ 
			while( mQueue.size()>0 && scheduleMapCount > 0){
				//pop mQueue
				//extract location of DN from TT id. remove from queue only if dn loc == tt loc
				//this condition should be skipped after few times. how ?
				//for project purpose, we can use random variable to enforce the condition
				//for software, we could lookup network monitoring and decide
				hbRes.addMapTasks(mQueue.removeFirst());
				scheduleMapCount--;
			}	
		}
		
		synchronized (rQueue) {
			while (rQueue.size()>0 && scheduleReducerCount > 0) {
				hbRes.addReduceTasks(rQueue.removeFirst());
				scheduleReducerCount--;
			}
		}
		//process mapStatus 
		for (int i = 0; i < hbReq.getMapStatusCount(); i++) {
			MapTaskStatus tempMapTaskStatus = hbReq.getMapStatus(i);
			if (tempMapTaskStatus.getTaskCompleted()) {
				int jobId = tempMapTaskStatus.getJobId();
				int taskId = tempMapTaskStatus.getTaskId();
				jobData ofJob = repo.get(jobId);
				int flagOfMapFinish = 0;
				synchronized (repo.get(jobId)) {
					for (int j = 0; j < ofJob.taskIds.size(); j++) {
						if (ofJob.taskIds.get(j) == taskId && !ofJob.flags.get(j)) {
							ofJob.flags.set(j, true);
							ofJob.setTaskCompleted(ofJob.getTaskCompleted() + 1);
							System.out.println("Seeting true"+ofJob.taskIds.get(j)+", completing "+ofJob.getTaskCompleted()+ " of total "+ ofJob.taskIds.size());
							System.out.println(ofJob.taskIds.get(j));
						}
					}
					if (ofJob.taskCompleted == ofJob.taskIds.size() && ofJob.getRedIds().size() ==0) {
						flagOfMapFinish = 1;
					}
				}
				if (flagOfMapFinish==1) {
					//populate reduce queue
					ArrayList<ReducerTaskInfo.Builder> newReds = new ArrayList<ReducerTaskInfo.Builder>();
					int numReducer = ofJob.getNumReducer();
					for (int j = 0; j < numReducer; j++) {
						newReds.add(ReducerTaskInfo.newBuilder().setJobId(jobId).setReducerName(ofJob.getReducerName()).setOutputFile(ofJob.getOpFile()));
						//assumed reducer id is not reduceTaskId
					}
					//scheduling
					//since we dont have partition concept, distributing to reducers in round robin fashion
					int index=0;
					for (int j = 0; j < ofJob.taskIds.size(); j++) {
						newReds.get(index).addMapOutputFiles("job_"+jobId+"_map_"+ofJob.taskIds.get(j));
						index = (index+1)%numReducer;
					}
					//now except reduce task ids, reducetaskinfo is ready and can be scheduled. So assign id and dispatch
					synchronized (taskCounter) {
						for (int j = 0; j < numReducer; j++) {
							taskCounter++;
							newReds.get(j).setTaskId(taskCounter);
							ofJob.getRedIds().add(taskCounter);
							ofJob.getRedFlags().add(false);
						}
					}
					synchronized (rQueue) {
						for (int j = 0; j < numReducer; j++) {
							rQueue.add(newReds.get(j).build());
						}
					}
				}
			}
		}
		
		//process ReduceStatus
		for (int i = 0; i < hbReq.getReduceStatusCount(); i++) {
			ReduceTaskStatus tempReduceTaskStatus = hbReq.getReduceStatus(i);
			if(tempReduceTaskStatus.getTaskCompleted()){
				int jobId = tempReduceTaskStatus.getJobId();
				int taskId = tempReduceTaskStatus.getTaskId();
				jobData ofJob = repo.get(jobId);
				synchronized (repo.get(jobId)) {
					for (int j = 0; j < ofJob.redIds.size(); j++) {
						if (ofJob.redIds.get(j) == taskId && !ofJob.redFlags.get(j)) {
							
							ofJob.redFlags.set(j, true);
							ofJob.setRedCompleted(ofJob.getRedCompleted()+1);
							System.out.println("Reduce setting to true "+taskId+ "count stands at "+ofJob.getRedCompleted()+" ,total "+ofJob.redIds.size());
						}
					}
					//if (ofJob.redCompleted == ofJob.redIds.size()) {
						//System.out.println("A job has been finished of id : "+jobId);
					//}
				}
			}
		}
		
		hbRes.setStatus(1);
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Got Exception in hb response");
			e.printStackTrace();
		}
		return hbRes.build().toByteArray();
		
	}

}
