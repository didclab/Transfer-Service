package org.onedatashare.transferservice.odstransferservice.constant;

import java.time.Duration;

public class ODSConstants {
    public static final String TIME = "time";
    public static final String SOURCE_HOST = "sourceURI";
    public static final String SOURCE_PORT = "sourcePort";
    public static final String CARBON_SCORE_SOURCE = "sourceCarbonScore";
    public static final String SOURCE_BASE_PATH = "sourceBasePath";
    public static final String DEST_BASE_PATH = "destBasePath";
    public static final String FILE_COUNT = "fileCount";
    public static final String SOURCE_CREDENTIAL_ID = "sourceCredential";
    public static final String DEST_CREDENTIAL_ID = "destCredential";
    public static final String SOURCE_CREDENTIAL_TYPE = "sourceCredentialType";
    public static final String DEST_CREDENTIAL_TYPE = "destCredentialType";
    public static final String DEST_HOST = "destURI";
    public static final String DEST_PORT = "destPort";
    public static final String CARBON_SCORE_DEST = "destCarbonScore";
    public static final String CHUNK_SIZE = "chunkSize";
    public static final String JOB_UUID = "jobUuid";
    public static final String OWNER_ID = "ownerId";
    public static final int SIXTYFOUR_KB = 64000;
    public static final int FIVE_MB = 5 * 1024 * 1024;
    public static final int TWENTY_MB = 20 * 1024 * 1024;
    public static final int GOOGLE_DRIVE_MIN_BYTES = 262144;
    public static final String RANGE = "Range";
    public static final String byteRange = "bytes=%s-%s";
    public static final String AccessControlExposeHeaders = "Access-Control-Expose-Headers";
    public static final String ContentRange = "Content-Range";
    public static final String SCP_COMMAND_REMOTE_TO_LOCAL = "scp -f ";
    public static final String SCP_COMMAND_LOCAL_TO_REMOTE = "scp -t ";
    public static final String SCP_MKDIR_CMD = "mkdir -p ";
    public static final String EXEC = "exec";
    public static final String JOB_SIZE = "jobSize";
    public static final String FILE_SIZE_AVG = "fileSizeAvg";

    public static final String ACCEPT_ENCODING = "accept-encoding";
    public static final String GZIP = "gzip";
    public static final String COMPRESS = "compress";
    public static final String CONCURRENCY = "concurrency";
    public static final String PARALLELISM = "parallelism";
    public static final String PIPELINING = "pipelining";
    public static final String OPTIMIZER = "optimizer";
    public static final String RETRY = "retry";
    public static final String APP_NAME = "appName";
    public static final String STEP_POOL_PREFIX = "step";
    public static final String PARALLEL_POOL_PREFIX = "parallel";
    public static final String SEQUENTIAL_POOL_PREFIX = "sequential";

    public static double computeThroughput(long totalBytes, Duration duration) {
        //use milliseconds by default but if milliseconds arent big enough then use nano seconds
        double megabits = totalBytes * 0.000008; //bytes to mb
        if(duration.toMillis() <= 1){
            return (megabits / duration.toNanos()) * 1000000000f; //working with nanoseconds and there are 1000000000 nanoseconds in a second
        }
        return (megabits / duration.toMillis()) * 1000f; //multiplied by 1000f for milli's to seconds
    }
}