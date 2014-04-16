package nl.vu.ict4d.marle.server.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * A simple class that can spot other server nodes.
 * @author RMH
 */
public class NodeSpotter {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    private static final String REQUEST_MSG = "MARLE_SRV_NODE_REQUEST";
    private static final int SERVER_PORT = 8888;
    private final int timeout;
    private DatagramSocket socket;

    /**
     * This will make a new nodespotter object with a default timeout of 15 seconds
     */
    public NodeSpotter() {
        this.timeout = 15000;
    }

    /**
     * Create a new nodespotter with a adjusted timeout.
     * @param timeout
     */
    public NodeSpotter(final int timeout) {
        this.timeout = timeout;
    }

    public Map<InetAddress, ServerNode> findNodes() throws IOException {
        // Make the return list
        Map<InetAddress, ServerNode> serverNodes = new HashMap<InetAddress, ServerNode>();

        // Find the server using UDP broadcast
        try {
            //Open a random port to send the package
            socket = new DatagramSocket();
            socket.setBroadcast(true);
            socket.setSoTimeout(timeout);

            byte[] sendData = REQUEST_MSG.getBytes();


            logger.debug("Started spotting for nodes, send multicast message");
            //Try the 255.255.255.255 first
//            try {
//                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName("255.255.255.255"), SERVER_PORT);
//                socket.send(sendPacket);
//            } catch (IOException e) {
//                throw new IOException("Count not send multicast message", e);
//            }

            int messagesSend = 0;

            // Broadcast the message over all the network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();

                // Dont send to loopback
                if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                    continue;
                }

                // Check for each interface address
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcastAddr = interfaceAddress.getBroadcast();
                    if (broadcastAddr == null) {
                        continue;
                    }

                    // Send the broadcast
                    try {
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, broadcastAddr, SERVER_PORT);
                        socket.send(sendPacket);
                        messagesSend++;
                    } catch (Exception e) {
                        logger.error("Could not send broadcast message on " + networkInterface.getDisplayName());
                    }
                }
            }

            // If no messages were send, try the old way
            if (messagesSend == 0) {
                logger.error("Couldn't send message on any interface, trying default.");
                //Try the 255.255.255.255 first
                try {
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName("255.255.255.255"), SERVER_PORT);
                    socket.send(sendPacket);
                } catch (IOException e) {
                    throw new IOException("Count not send multicast message", e);
                }
            }

            // Wait for a response
            byte[] recvBuf = new byte[51];
            DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);

            // Get the local address
            InetAddress localhost = InetAddress.getLocalHost();
            ServerNode srvNode;
            while (true) {
                socket.receive(receivePacket);

                // Skip the localhost
                if (receivePacket.getAddress().getHostAddress().equals(localhost.getHostAddress())) {
                    continue;
                }

                //Check if the message is correct
                srvNode = ServerNode.fromBytes(receivePacket.getData());
                if (srvNode != null) {
                    //We have a valid response
                    logger.debug("Found node at " + receivePacket.getAddress().getHostAddress());

                    serverNodes.put(receivePacket.getAddress(), srvNode);
                }

            }

            //Close the port!
            //c.close();
        } catch (SocketTimeoutException ste) {
            logger.debug(" - Wait time exceeded, found nodes " + serverNodes.size());
            return serverNodes;
        }
    }
}
