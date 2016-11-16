/**
 * 
 */
package Common;

/**
 * @author Mayank Dave
 *
 */
public final class Constants {
	public static final int BLOCK_SIZE = 335544;//33554432;
    public static final int NUM_DN = 4;
    public static final int NN_PORT = 55005;
    public static final String NN_IP = "127.0.0.1";
    public static final String NN_NAME = "NAMENODE";
    public static final String[] DN_IPS = {"127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1"};
    //public static final String[] DN_IPS = {"10.0.0.1","10.0.0.2","10.0.0.3","10.0.0.4"};
    public static final int[] DN_PORTS = {55001,55002,55003,55004};
    public static final String[] DN_NAME = {"DN1","DN2","DN3","DN4"};
    
    public static final int heartbeatSleep = 30000;
    public static final int blockReportSleep = 1000;
    
    public static final String fileConfigPath = "fileConfig.txt";
    public static final String dataNodeConfig = "dataConfig.txt";
    
    public static final int CLIENT_SLEEP = 3000;
}
