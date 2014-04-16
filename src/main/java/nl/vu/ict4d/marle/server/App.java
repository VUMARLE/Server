package nl.vu.ict4d.marle.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import nl.vu.ict4d.marle.archive.Archive;
import nl.vu.ict4d.marle.server.data.ServerSocketThread;
import nl.vu.ict4d.marle.server.data.sync.FileReplicator;
import nl.vu.ict4d.marle.server.data.sync.ReplicateStack;
import nl.vu.ict4d.marle.server.multicast.NetworkThread;
import nl.vu.ict4d.marle.server.multicast.NodeSpotter;
import nl.vu.ict4d.marle.server.multicast.ServerNode;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 */
public class App implements MarleManager {

    private static Logger logger; // Created in startup method!
    private ReplicateStack replicatestack;
    private ServerSocketThread serversocket;
    private FileReplicator replicator;
    private NetworkThread networkthread;
    private Archive archive;
    private UUID serverid;

    public App() {
    }

    public static void main(String[] args) {
        new App().startup();
    }

    /**
     * This method will startup the server
     */
    public void startup() {
        if (serversocket != null) {
            throw new IllegalStateException("Server already started up.");
        }

        // =====================================
        // Logger initialization
        // =====================================
        try {
            PropertyConfigurator.configure(getLogConfig());
            logger = Logger.getLogger("MarleLogger");
        } catch (Exception ex) {
            System.out.println("Could not initialize logger..");
            System.exit(500);
            return;
        }

        logger.info("========================");
        logger.info("Starting MarleServer....");
        // =====================================
        // Load Configuration
        // =====================================

        // TODO!
        try {
            serverid = UUID.nameUUIDFromBytes(java.net.Inet4Address.getLocalHost().getAddress());
        } catch (UnknownHostException ex) {

            // Just get a random one
            serverid = UUID.randomUUID();
            System.out.println("Could not generate from local address, using random UUID.");
        }

        // =====================================
        // Setup archive and replicate stack
        // =====================================
        try {
            this.archive = new Archive();
        } catch (IOException ex) {
            logger.info("Could not load the archive.");
        }

        this.replicatestack = new ReplicateStack();

        // =====================================
        // Neightbourhood scan (just to inform what is in the network)
        // =====================================

        try {
            logger.info("Quick network scan..");
            Map<InetAddress, ServerNode> addresses = new NodeSpotter(5000).findNodes();
            logger.info("Found " + addresses.size() + " nodes in the network.");
        } catch (IOException ex) {
            logger.fatal("Could not scan the network.", ex);
            System.exit(502);
            return;
        }
        
        // =====================================
        // Start the networking thread (Multicaster)
        // =====================================
        try {
            logger.info("Starting multicaster..");
            networkthread = new NetworkThread(this);
            networkthread.start();

        } catch (IOException ex) {
            logger.fatal("Could not start the background network thread.", ex);
            System.exit(502);
            return;
        }

        // =====================================
        // Start the Socket thread (Data transfers)
        // =====================================
        logger.info("Starting data transfer listener..");
        serversocket = new ServerSocketThread(this);
        serversocket.start();

        // =====================================
        // Starting file replication thread
        // =====================================
        logger.info("Starting replicator..");
        replicator = new FileReplicator(this);
        replicator.start();

        // =====================================
        // Update?
        // =====================================

        // Do something with the addresses?
        // Maybe registering ourselves as new server node or ask for updates


        // =====================================
        // Loading complete!
        // =====================================

        logger.info("--------------------------------");
        logger.info("MarleServer succesfully started!");
        logger.info("--------------------------------");
    }

    /**
     * Looks up the properties file with the logconfig
     * @return
     */
    private static Properties getLogConfig() {
        try {
            Properties prop = new Properties();
            InputStream stream = null;
            try {
                // Load from internal files
                stream = App.class.getResourceAsStream("/defaults/logconfig.properties");
                prop.load(stream);
                prop.setProperty("log4j.appender.fileapp.File", new File("applog.log").getAbsolutePath());
            } finally {
                try {
                    if (stream != null)
                        stream.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            return prop;
        } catch (IOException ioe) {
            //System.out.println("Exception getting log configuration!");
            System.exit(503);
            return null;
        }
    }

    /**
     * Gets the server UUID.
     * @return
     */
    @Override
    public UUID getServerUUID() {
        return serverid;
    }

    @Override
    public Archive getArchive() {
        return archive;
    }

    /**
     * The active socket thread in the background.
     * @return
     */
    @Override
    public ServerSocketThread getSocketThread() {
        return serversocket;
    }

    /**
     * The active network thread in the background.
     * @return
     */
    @Override
    public NetworkThread getNetworkThread() {
        return networkthread;
    }

    /**
     * The stack with items that need to be replicated to other servers.
     */
    @Override
    public ReplicateStack getReplicationStack() {
        return replicatestack;
    }

    @Override
    public FileReplicator getReplicator() {
        return replicator;
    }
}
