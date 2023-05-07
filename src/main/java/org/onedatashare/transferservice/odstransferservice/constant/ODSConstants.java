package org.onedatashare.transferservice.odstransferservice.constant;

import java.time.Duration;

public class ODSConstants {
    public static final String DROPBOX_URI_SCHEME = "dropbox://";
    public static final String DRIVE_URI_SCHEME = "gdrive://";
    public static final String BOX_URI_SCHEME = "box://";
    public static final String AMAZONS3_URI_SCHEME = "amazons3://";
    public static final String SFTP_URI_SCHEME = "sftp://";
    public static final String FTP_URI_SCHEME = "ftp://";
    public static final String SCP_URI_SCHEME = "scp://";
    public static final String GRIDFTP_URI_SCHEME = "gsiftp://";
    public static final String HTTP_URI_SCHEME = "http://";
    public static final String HTTPS_URI_SCHEME = "https://";
    public static final String DROPBOX_CLIENT_IDENTIFIER = "OneDataShare-DIDCLab";
    public static final String FILE_SIZE = "fileSize";
    public static final String FILE_PATH = "filePath";
    public static final String FILE_ID = "file_id";

    public static final String  DROPBOX= "dropbox";
    public static final String  GOOGLEDRIVE = "gdrive";
    public static final String  BOX= "box";
    public static final String TIME = "time";
    public static final String SOURCE_ACCOUNT_ID_PASS = "sourceAccountIdPass";
    public static final String SOURCE_HOST = "sourceURI";
    public static final String SOURCE_PORT = "sourcePort";
    public static final String DESTINATION_ACCOUNT_ID_PASS = "destinationAccountIdPass";
    public static final String SOURCE_BASE_PATH = "sourceBasePath";
    public static final String DEST_BASE_PATH = "destBasePath";
    public static final String SOURCE = "source";
    public static final String FILE_COUNT = "fileCount";
    public static final String SOURCE_CREDENTIAL_ID = "sourceCredential";
    public static final String DEST_CREDENTIAL_ID = "destCredential";
    public static final String SOURCE_CREDENTIAL_TYPE = "sourceCredentialType";
    public static final String DEST_CREDENTIAL_TYPE = "destCredentialType";
    public static final String DEST_HOST = "destURI";
    public static final String DEST_PORT = "destPort";
    public static final String INFO_LIST = "infoList";
    public static final String PRIORITY = "priority";
    public static final String CHUNK_SIZE = "chunkSize";
    public static final String OWNER_ID = "ownerId";
    public static final String TRANSFER_OPTIONS = "transferOptions";
    public static final int SIXTYFOUR_KB = 64000;
    public static final int TRANSFER_SLICE_SIZE = 1 << 20;
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
    public static final String CONTENT_ENCODING = "content-encoding";
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