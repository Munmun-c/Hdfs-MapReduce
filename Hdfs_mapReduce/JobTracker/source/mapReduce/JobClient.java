package mapReduce;

import static mapReduce.MRConstants.JCLIENT_SLEEP;
import static mapReduce.MRConstants.JOBSTATUS_SLEEP;
import static mapReduce.MRConstants.JT_IP;
import static mapReduce.MRConstants.JT_NAME;
import static mapReduce.MRConstants.JT_PORT;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.google.protobuf.InvalidProtocolBufferException;

import mapReduce.MapReduce.JobStatusRequest;
import mapReduce.MapReduce.JobStatusResponse;
import mapReduce.MapReduce.JobSubmitRequest;
import mapReduce.MapReduce.JobSubmitResponse;

public class JobClient {

	public JobClient() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Registry registryJT;
		try {
			//not reading jt location from conf file, instead using a constants java file
			registryJT = LocateRegistry.getRegistry(JT_IP,JT_PORT );
			IJobTracker JT_Stub = (IJobTracker) registryJT.lookup(JT_NAME);
			
			try {
				Thread.sleep(JCLIENT_SLEEP);                 
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			
			JobSubmitRequest.Builder jobreq = JobSubmitRequest.newBuilder();
			jobreq.setMapName(args[0]);
			jobreq.setReducerName(args[1]);
			jobreq.setInputFile(args[2]);
			jobreq.setOutputFile(args[3]);
			jobreq.setNumReduceTasks(Integer.parseInt(args[4]));
			
			byte temp[] = JT_Stub.jobSubmit(jobreq.build().toByteArray());
			JobSubmitResponse jobres = JobSubmitResponse.parseFrom(temp) ;
			
			if (jobres.getStatus() != 1) {
				System.out.println("Exiting, Got error status for submitted job : "+ jobres.getStatus());
			}
			else{
				int jobId = jobres.getJobId();
				System.out.println("Job submitted. Job id is : " + jobId );
				while(true){
					try {
						Thread.sleep(JOBSTATUS_SLEEP);                 
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					
					temp = JT_Stub.getJobStatus(JobStatusRequest.newBuilder().setJobId(jobId).build().toByteArray());
					JobStatusResponse jsResp = JobStatusResponse.parseFrom(temp);
					
					if (jsResp.getStatus() != 1) {
						System.out.println("Some error in status response : "+ jsResp.getStatus());
					}
					else {
						//jsResp.getTotalMapTasks() should never be zero
						System.out.print("Completion map tasks: "+ (100.0*jsResp.getNumMapTasksStarted())/jsResp.getTotalMapTasks()+ "  ");
						System.out.println("Completion Reduce tasks: "+ (100.0*jsResp.getNumReduceTasksStarted())/jsResp.getTotalReduceTasks());
						if (jsResp.getJobDone()) {
							break;
						}
					}
				}
				System.out.println("Job successfully completed");
				
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
