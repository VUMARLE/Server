package nl.vu.ict4d.marle.server.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.UUID;
import nl.vu.ict4d.marle.server.MarleManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

/**
 *
 * @author RMH
 */
public class NetworkThread extends Thread {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    private static final String REQUEST_MSG = "MARLE_SRV_NODE_REQUEST";
    private static final int SERVER_PORT = 8888;
    protected DatagramSocket socket = null;
    private final MarleManager servermanager;

    public NetworkThread(MarleManager servermanager) throws IOException {
        this(servermanager, "MARLE MCT");
    }

    private NetworkThread(MarleManager servermanager, String name) throws IOException {
        super(name);
        this.servermanager = servermanager;
    }

    @Override
    public void run() {
        try {
            //Keep a socket open to listen to all the UDP trafic that is destined for this port
            socket = new DatagramSocket(SERVER_PORT, InetAddress.getByName("0.0.0.0"));
            socket.setBroadcast(true);

            logger.log(Priority.INFO, "Listening for new nodes in network.");

            // Get the local address
            InetAddress localhost = InetAddress.getLocalHost();

            while (true) {
                // Receive a packet
                byte[] recvBuf = new byte[32];
                DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
                socket.receive(packet);

                // Skip the localhost (own packets)
                if (packet.getAddress().getHostAddress().equals(localhost.getHostAddress())) {
                    continue;
                }

                //Packet received
                logger.log(Priority.DEBUG, "Incomming connection from: " + packet.getAddress().getHostAddress());

                //See if the packet holds the right command (message)
                String message = new String(packet.getData()).trim();
                if (message.equals(REQUEST_MSG)) {
                    // Collect serverdata
                    byte[] sendData = new ServerNode(servermanager.getServerUUID(),
                            servermanager.getArchive().getArchiveFreeSpace(),
                            servermanager.getSocketThread().getActiveConnections()).toBytes();

                    //Send a response
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(), packet.getPort());
                    socket.send(sendPacket);

                    logger.log(Priority.DEBUG, "Responded to: " + sendPacket.getAddress().getHostAddress());
                }
            }
        } catch (IOException ex) {
            logger.log(Priority.ERROR, "Error in networkthread!", ex);
        }
    }
}
