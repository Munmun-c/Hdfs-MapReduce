package mapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote{
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] jobReq) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] statusReq) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] beatReq) throws RemoteException;
}
