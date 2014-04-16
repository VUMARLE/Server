package nl.vu.ict4d.marle.server.data.sync;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import nl.vu.ict4d.marle.server.MarleManager;
import nl.vu.ict4d.marle.server.data.ClientSocket;
import nl.vu.ict4d.marle.server.file.FileMeta;
import nl.vu.ict4d.marle.server.multicast.NodeSpotter;
import nl.vu.ict4d.marle.server.multicast.ServerNode;
import org.apache.log4j.Logger;

/**
 *
 * @author RMH
 */
public class FileReplicator extends Thread {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    /** The amount of MS the thread will sleep before retrying to replicate files. */
    private static final long SLEEP_TIME_MS = 5 * 60 * 1000; // 5 min
    /** The amount of MS the thread will wait if no server nodes were found in the network. */
    private static final long SLEEP_TIME_NO_NODES_MS = 5 * 60 * 1000; // 5 min
    /**
     * How many replications requests are handled per cycle. Note that it will still handle a
     * lower amount of marked files, this will only limit the amount he does in a single loop
     * before refreshing resources.
     */
    private static final int REPLICATIONS_PER_CYCLE = 4;
    private final MarleManager srvmgr;

    public FileReplicator(MarleManager servermanger) {
        this.srvmgr = servermanger;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        while (true) {
            List<UUID> repIdList = null;
            try {
                try {
                    // Only sleep if the stack is empty
                    if (srvmgr.getReplicationStack().getItemsOnStack() == 0)
                        Thread.sleep(SLEEP_TIME_MS);
                } catch (InterruptedException ie) {
                    logger.info("Replicator interrupted, starting new loop.");
                }

                // Get the list of items to replicate.
                if (repIdList == null)
                    repIdList = srvmgr.getReplicationStack().getItems(REPLICATIONS_PER_CYCLE);

                // Check if we got files.
                if (!repIdList.isEmpty()) {
                    logger.debug("Replicating " + repIdList.size() + " files");

                    // Find the servers
                    Map<InetAddress, ServerNode> networkNodes = new NodeSpotter().findNodes();

                    // check if we got nodes in the network
                    if (networkNodes.isEmpty()) {
                        // If no nodes found, skip
                        logger.error("No server nodes found! Cannot replicate files!");

                        // Readd to the replication stack
                        for (UUID fileid : repIdList) {
                            srvmgr.getReplicationStack().addFileForReplication(fileid);
                        }

                        repIdList = null;
                        networkNodes = null;

                        // Sleep for a while, maybe server nodes get back up after.
                        try {
                            Thread.sleep(SLEEP_TIME_NO_NODES_MS);
                        } catch (InterruptedException ie) {
                            logger.info("Replicator interrupted while waiting for nodes to come online");
                        }

                    } else {
                        // Loop through each of the file marked for replication
                        for (UUID fileid : repIdList) {
                            replicateFile(networkNodes, fileid);
                        }
                    }
                }

                // Clear variable for next run
                repIdList = null;
            } catch (IOException ex) {
                logger.fatal("Exception when replicating files, failed to replicate some files.", ex);
            }
        }
    }

    /**
     * This will lookup the servernode that is best suitable to push files to.
     * @param networkNodes
     * @return Address of the best-target servernode
     */
    private InetAddress selectTargetServer(Map<InetAddress, ServerNode> networkNodes) {
        InetAddress targetAddress = null;
        long freespace = 0;

        // Loop through the list to find the servernode to push to
        for (Map.Entry<InetAddress, ServerNode> entry : networkNodes.entrySet()) {
            InetAddress inetAddress = entry.getKey();
            ServerNode serverNode = entry.getValue();

            // Check if this server got more freespace
            if (serverNode.getFreespace() > freespace) {
                targetAddress = inetAddress;
                freespace = serverNode.getFreespace();
            }
        }
        return targetAddress;
    }

    /**
     * Replicates the given fileid to one of the servernodes.
     * @param networkNodes
     * @param fileid
     * @throws IOException
     */
    private void replicateFile(Map<InetAddress, ServerNode> networkNodes, UUID fileid) throws IOException {
        ClientSocket socket = null;

        // Get the best server node
        InetAddress targetServer = this.selectTargetServer(networkNodes);
        ServerNode serverinfo = networkNodes.get(targetServer);
        // Get the meta file (will be updated later on
        FileMeta meta = srvmgr.getArchive().getFileMeta(fileid);
        // Add target server to meta
        meta.getServerlocations().add(serverinfo.getId());

        logger.debug("Replicating file '" + fileid + "' to server '" + serverinfo.getId() + "'.");

        // Send the actual file
        try {
            socket = new ClientSocket(true);
            socket.openConnection(targetServer);

            // Send the file to the other server
            if (socket.sendFile(meta, srvmgr.getArchive().getFileObject(fileid))) {
                logger.debug("File replicated to " + serverinfo.getId());
            }
        } catch (IOException ex) {
            logger.error("Error replicating file " + fileid, ex);
            // Readd for replication
            srvmgr.getReplicationStack().addFileForReplication(fileid);
            return;
        } finally {
            try {
                if (socket != null)
                    socket.closeConnection();
            } catch (IOException ex) {
            }
        }

        // Update the freespace of the other server (just the local reference)
        serverinfo.setFreespace(serverinfo.getFreespace() - srvmgr.getArchive().getFileSize(fileid));

        try {
            srvmgr.getArchive().updateContentFile(meta);

            // Update the meta of the other servers
            for (InetAddress addr : networkNodes.keySet()) {
                // The targetserver already got the update
                if (!addr.equals(targetServer)) {
                    try {
                        socket = new ClientSocket(true);
                        socket.sendMetaUpdate(meta);
                    } catch (IOException ex) {
                        logger.error("Could not send meta update to " + addr.getHostAddress(), ex);
                        continue;
                    } finally {
                        try {
                            if (socket != null)
                                socket.closeConnection();
                        } catch (IOException ex) {
                        }
                    }
                }
            }
        } catch (IOException ex) {
            logger.error("Could not update metadata for the file " + fileid, ex);
        }
    }
}
