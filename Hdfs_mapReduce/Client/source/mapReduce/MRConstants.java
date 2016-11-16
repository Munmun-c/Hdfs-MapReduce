package mapReduce;

public final class MRConstants {
    public static final int NUM_TT = 4;
    public static final int JT_PORT = 54005;
    public static final String JT_IP = "127.0.0.1";
    public static final String JT_NAME = "JOBTRACKER";
    public static final String[] TT_IPS = {"127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1"};
    //public static final String[] TT_IPS = {"10.0.0.1","10.0.0.2","10.0.0.3","10.0.0.4"};
    public static final int[] TT_PORTS = {54001,54002,54003,54004};
    public static final String[] TN_NAME = {"TT1","TT2","TT3","TT4"};
    
    public static final int TT_NUM_MTHREADS = 5; 
    public static final int TT_NUM_RTHREADS = 5;
    
    public static final int JCLIENT_SLEEP = 3000;
    public static final int JOBSTATUS_SLEEP = 5000;
    public static final int THREAD_POLL = 1000;
    public static final int HB_SLEEP = 1000;
    public static final String GREP_STRING_FILE = "grep_input.config";
    public static final String JOB_COUNTER_FILE = "last_jobCounter.config";
}
