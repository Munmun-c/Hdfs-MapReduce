package mapReduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.google.protobuf.ByteString;

import Common.Hdfs;
import Common.Hdfs.AssignBlockRequest;
import Common.Hdfs.AssignBlockResponse;
import Common.Hdfs.BlockLocationRequest;
import Common.Hdfs.BlockLocationResponse;
import Common.Hdfs.CloseFileRequest;
import Common.Hdfs.CloseFileResponse;
import Common.Hdfs.OpenFileRequest;
import Common.Hdfs.OpenFileResponse;
import Common.Hdfs.ReadBlockRequest;
import Common.Hdfs.ReadBlockResponse;
import Common.Hdfs.WriteBlockRequest;
import Common.Hdfs.WriteBlockResponse;
import IDataNode.IDataNode;
import INameNode.INameNode;

import static Common.Constants.*
;
public class Helper_class {
	Registry registryNN;	
	INameNode NN_Stub ;
	IDataNode[] DN_Stubs;
	
public Helper_class() {
		super();
		DN_Stubs = new IDataNode[4]; 
	}
public void initHelper(){
	try {
		registryNN = LocateRegistry.getRegistry(NN_IP,NN_PORT );
		 NN_Stub=(INameNode) registryNN.lookup(NN_NAME);

			Registry DNRegistries[] = new Registry[DN_IPS.length];
			//IDataNode DN_Stubs[] = new IDataNode[DN_IPS.length];
			for(int i=0; i<DN_IPS.length;i++){
				DNRegistries[i] = LocateRegistry.getRegistry(DN_IPS[i],DN_PORTS[i]);
				if (DNRegistries[i] == null) {
					System.out.println("Got null getregistry "+ DNRegistries[i]+"for "+i);
				}
		//		else {
		//			System.out.println("DN registry " + DNRegistries[i]);
	//			}
				//DN_Stubs[i] = (IDataNode) DNRegistries[i].lookup(DN_NAME[i]);
			}
			IDataNode temp1,temp2,temp3,temp4;
	//		System.out.println(DN_NAME[0]);
			temp1=(IDataNode) DNRegistries[0].lookup(DN_NAME[0]);
		//	System.out.println(DN_NAME[1]);
			temp2=(IDataNode) DNRegistries[1].lookup(DN_NAME[1]);
		//	System.out.println(DN_NAME[2]);
			temp3=(IDataNode) DNRegistries[2].lookup(DN_NAME[2]);
			//System.out.println(DN_NAME[3]);
			temp4=(IDataNode) DNRegistries[3].lookup(DN_NAME[3]);
		     DN_Stubs[0] =temp1;
		     DN_Stubs[1]=temp2;
		     DN_Stubs[2]=temp3;
		     DN_Stubs[3]=temp4;
	} catch (RemoteException | NotBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

void get(String file) throws IOException
{
	byte[] myBuffer = new byte[BLOCK_SIZE];
	int bytesRead = 0;
	
	OpenFileRequest or_get=Hdfs.OpenFileRequest.newBuilder().setFileName(file).setForRead(true).build();
	byte[] inp_=or_get.toByteArray();
	byte[] op_= NN_Stub.openFile(inp_);
	OpenFileResponse or2_=OpenFileResponse.parseFrom(op_);
	if (or2_.getStatus() != 1) {
		System.out.println("Error in file open - status = " + or2_.getStatus());
	}
	else {
    	int handle_=or2_.getHandle();
    	BlockLocationRequest.Builder Br=Hdfs.BlockLocationRequest.newBuilder();
    	/*for(int i=0;i<or2_.getBlockNumsCount();i++)
    	{
    		int Block_num=or2_.getBlockNums(i);
    		or2_.g
    		Br.addBlockNums(Block_num);
    		//Br.setBlockNums(i, Block_num);
    	}*/
    	Br.addAllBlockNums(or2_.getBlockNumsList());
    	inp_=Br.build().toByteArray();
		op_=NN_Stub.getBlockLocations(inp_);
		BlockLocationResponse Blr=BlockLocationResponse.parseFrom(op_);
		if (Blr.getStatus() != 1) {
			System.out.println("Aborting .. Error in BlockLocation request - status "+Blr.getStatus());
			//Restart of Namenode required for being able to put same filenamme again
			System.exit(-1);
		}
		//we have location of each block, now make readblock request for each block
		//and append to the system file
		BufferedOutputStream outFile = new BufferedOutputStream(new FileOutputStream(file));
		for (int i = 0; i < Blr.getBlockLocationsCount(); i++) {
			
			String ip1_=Blr.getBlockLocations(i).getLocations(0).getIp();
			int port1_=Blr.getBlockLocations(i).getLocations(0).getPort();
    		String ip2_=Blr.getBlockLocations(i).getLocations(1).getIp();
    		int port2_=Blr.getBlockLocations(i).getLocations(1).getPort();
    		ReadBlockRequest RB=Hdfs.ReadBlockRequest.newBuilder().setBlockNumber(or2_.getBlockNums(i)).build();
    		
    		int index=0;
	    	int matchFlag=0;
	    	for(;index<DN_IPS.length;index++){
	    		if (DN_IPS[index].equals(ip1_) && DN_PORTS[index] == port1_) {
					matchFlag=1;
					break;
				}
	    	}
	    	if (matchFlag!=1) {
				System.out.println("Ip match not found");
			}
	    	byte[] myOp =DN_Stubs[index].readBlock(RB.toByteArray());
	    	ReadBlockResponse wb;
	    	wb =ReadBlockResponse.parseFrom(myOp);
	    	if (wb.getStatus()!= 1) {
				//try on another DN, before aborting
	    		int index1=0;
		    	int matchFlag1=0;
		    	for(;index1<DN_IPS.length;index1++){
		    		if (DN_IPS[index1].equals(ip2_) && DN_PORTS[index] == port2_) {
						matchFlag1=1;
						break;
					}
		    	}
		    	if (matchFlag1!=1) {
					System.out.println("Ip match not found");
				}
		    	byte[] myOp1 =DN_Stubs[index].readBlock(RB.toByteArray());
		    	wb=ReadBlockResponse.parseFrom(myOp1);
		    	if (wb.getStatus()!= 1) {
					System.out.println("Aborting .. Error in readblock request - status "+wb.getStatus());
					//Restart of Namenode required for being able to put same filenamme again
					System.exit(-1);
				}
			}
	    	
			//we have response in wb, now append in file
	    	//System.out.println("Amt of data received is "+wb.getDataList().size());
	    	for (int j = 0; j < wb.getDataList().size(); j++) {
				wb.getDataList().get(j).copyTo(myBuffer, j);
				//System.out.print(myBuffer[j]);
			}
	    	//System.out.println();
	    	//wb.getData(j).copyTo(myBuffer, 0);
	    	outFile.write(myBuffer, 0, wb.getDataCount());
		}
		outFile.close();
		
		//close file
    	CloseFileRequest cf_=Hdfs.CloseFileRequest.newBuilder().setHandle(handle_).build();
    	inp_=cf_.toByteArray();
    	op_=NN_Stub.closeFile(inp_);
    	CloseFileResponse cfr_=CloseFileResponse.parseFrom(op_);
    	int st_cfr=cfr_.getStatus();
    	if(st_cfr != 1){
    		System.out.println("Error in close file request, status - "+ st_cfr);
    	}
	}
}
void put(String file) throws IOException
{
	byte[] myBuffer = new byte[BLOCK_SIZE];
	int bytesRead = 0;
	BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
	
	OpenFileRequest or1=Hdfs.OpenFileRequest.newBuilder().setFileName(file).setForRead(false).build();
	byte[] inp=or1.toByteArray();
	byte[] op= NN_Stub.openFile(inp);
	OpenFileResponse or2=OpenFileResponse.parseFrom(op);
	if (or2.getStatus() != 1) {
		System.out.println("Error in file open - status = " + or2.getStatus());
	}
	else {
		int handle=or2.getHandle();
		while ((bytesRead = in.read(myBuffer, 0, BLOCK_SIZE)) != -1)
    	{
			WriteBlockRequest.Builder wbrBuilder =Hdfs.WriteBlockRequest.newBuilder();
			//prepare data for writeblock request
			//data to write
			//ByteString value = ByteString.copyFrom(myBuffer, 0, bytesRead);// From(myBuffer);
			for (int i = 0; i < bytesRead; i++) {
				wbrBuilder.addData(ByteString.copyFrom(myBuffer, i, 1));
			}
			
			//blockLocation to be used
			AssignBlockRequest ab=Hdfs.AssignBlockRequest.newBuilder().setHandle(handle).build();
	    	inp=ab.toByteArray();
	    	op=NN_Stub.assignBlock(inp);
	    	AssignBlockResponse abr=AssignBlockResponse.parseFrom(op);
	    	if (abr.getStatus()!= 1) {
				System.out.println("Aborting .. Error in assignblock request - status "+abr.getStatus());
				//Restart of Namenode required for being able to put same filenamme again
				System.exit(-1);
			}
	    	wbrBuilder.setBlockInfo(abr.getNewBlock());
	    	inp = wbrBuilder.build().toByteArray();
	    	
	    	//get DN details
	    	String ip1=abr.getNewBlock().getLocationsList().get(0).getIp();
	    	int port1=abr.getNewBlock().getLocationsList().get(0).getPort();
	    	//System.out.println("Num locn from assignreq is " +abr.getNewBlock().getLocationsCount());
	    	//System.out.println(abr.getNewBlock().getLocationsList().get(1).getIp()+" and port"+abr.getNewBlock().getLocationsList().get(1).getPort());
	    	int index=0;
	    	int matchFlag=0;
	    	for(;index<DN_IPS.length;index++){
	    		if (DN_IPS[index].equals(ip1) && DN_PORTS[index]==port1) {
					matchFlag=1;
					break;
				}
	    	}
	    	if (matchFlag!=1) {
				System.out.println("Ip match not found");
			}
	    	op=DN_Stubs[index].writeBlock(inp);
	    	WriteBlockResponse wb=WriteBlockResponse.parseFrom(op);
	    	if (wb.getStatus()!= 1) {
				System.out.println("Aborting .. Error in assignblock request - status "+abr.getStatus());
				//Restart of Namenode required for being able to put same filenamme again
				System.exit(-1);
			}
	    	
	    	 
    	}
		CloseFileRequest cf=Hdfs.CloseFileRequest.newBuilder().setHandle(handle).build();
    	inp=cf.toByteArray();
    	op=NN_Stub.closeFile(inp);
    	CloseFileResponse cfr=CloseFileResponse.parseFrom(op);
    	if(cfr.getStatus() != 1){
    		System.out.println("Error in close file request, status - "+ cfr.getStatus());
    	}
	}
	in.close();
}

}

