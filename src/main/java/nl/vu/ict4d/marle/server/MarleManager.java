package nl.vu.ict4d.marle.server;

import java.util.UUID;
import nl.vu.ict4d.marle.archive.Archive;
import nl.vu.ict4d.marle.server.data.ServerSocketThread;
import nl.vu.ict4d.marle.server.data.sync.FileReplicator;
import nl.vu.ict4d.marle.server.data.sync.ReplicateStack;
import nl.vu.ict4d.marle.server.multicast.NetworkThread;

/**
 * Small interface for the class that is managing the items running in the background.
 * @author RMH
 */
public interface MarleManager {

    /**
     * Returns the active archive object
     */
    Archive getArchive();

    /**
     * The socketthread that is running in the background
     */
    ServerSocketThread getSocketThread();

    /**
     * The networking thread that is running in the background
     */
    NetworkThread getNetworkThread();
    
    /**
     * The stack with items that need to be replicated to other servers.
     */
    ReplicateStack getReplicationStack();

    /**
     * Gets the server UUID.
     * @return
     */
    UUID getServerUUID();

    /**
     * The filereplicator responsible for replicating files to other servers.
     */
    FileReplicator getReplicator();
}
