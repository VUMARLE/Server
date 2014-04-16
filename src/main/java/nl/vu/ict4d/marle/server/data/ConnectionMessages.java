package nl.vu.ict4d.marle.server.data;

/**
 *
 * @author RMH
 */
interface ConnectionMessages {
    
    // Initial message as response to a connection request
    static final String SERVER_RESPONSE_BUSY = "RES_BUSY";
    static final String SERVER_RESPONSE_ACCEPT = "RES_ACCE";
    
    // Requests from the client to a different server
    static final String SERVER_FILE_PULL = "FILE_PUL";
    static final String SERVER_FILE_PUSH = "FILE_PSH";
    static final String SERVER_FILE_DELETE = "FILE_DEL";
    static final String SERVER_META_DOWNLOAD = "META_GET";
    static final String SERVER_META_UPDATE = "META_UPD";
    
    
    /** First bit for response on filerequest noting that the file exists and is send. **/
    static final int SERVER_FILEPULL_SENDING = 1;
    /** First bit for response on filerequest noting that the file does not exist.**/
    static final int SERVER_FILEPULL_MISSING = 2;
    
    static final int SERVER_FILEPUSH_ACCEPT = 1;
    static final int SERVER_FILEPUSH_DUPELICATE = 2;
    static final int SERVER_FILEPUSH_NOFREESPACE= 3;
    
    static final int SERVER_FILEDELETE_DELETED = 1;
    static final int SERVER_FILEDELETE_MISSING = 2;
    
    
    static final int SERVER_ERROR = 200;
    
}
