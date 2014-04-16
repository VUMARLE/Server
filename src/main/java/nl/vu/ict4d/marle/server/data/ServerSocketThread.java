package nl.vu.ict4d.marle.server.data;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import nl.vu.ict4d.marle.server.MarleManager;
import org.apache.log4j.Logger;

/**
 *
 * @author RMH
 */
public class ServerSocketThread extends Thread {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    private static final int SERVER_DATA_SOCKET = 12333;
    private static final int MAX_ACTIVE_CONNECTIONS = 10;
    private final MarleManager servermanager;
    private final ConnectionHandler[] datahandlers = new ConnectionHandler[MAX_ACTIVE_CONNECTIONS];
    private ServerSocket serverSocket;

    public ServerSocketThread(MarleManager servermanager) {
        this.servermanager = servermanager;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(SERVER_DATA_SOCKET);

            while (true) {
                // wait for a client request and when received assign it to the
                // client socket
                // when u wanna send data to that client u'll use this socket
                Socket clientConnection = serverSocket.accept();

                logger.debug("Incomming connection from: " + clientConnection.getInetAddress().getHostAddress());

                ConnectionHandler handler = null;
                // Find an empty spot for handling data;
                for (int i = 0; i < datahandlers.length; i++) {
                    if (datahandlers[i] == null) {
                        datahandlers[i] = new ConnectionHandler(i, this);
                        handler = datahandlers[i];
                        break;
                    }
                }

                // Check if we are full
                if (handler != null) {
                    logger.debug("Connection accepted, send to handler.");
                    clientConnection.getOutputStream().write(ConnectionMessages.SERVER_RESPONSE_ACCEPT.getBytes());
                    clientConnection.getOutputStream().flush();
                    handler.handle(clientConnection);
                } else {
                    // Server is busy, reject                
                    try {
                        logger.debug("Connection refused, server to busy!");
                        clientConnection.getOutputStream().write(ConnectionMessages.SERVER_RESPONSE_BUSY.getBytes());
                        clientConnection.close();
                    } catch (IOException ex) {
                        logger.error("Could not close client connection!", ex);
                    }
                }
                clientConnection = null;
            }

        } catch (IOException ex) {
            logger.fatal("Could not open serversocket for data connections.");
        }
    }

    /**
     * Returns the amount of connections that are currently active.
     */
    public int getActiveConnections() {
        int activeConnections = 0;
        for (ConnectionHandler connectionHandler : datahandlers) {
            if (connectionHandler == null)
                activeConnections++;
        }
        return activeConnections;
    }

    /**
     * Notifies the serverSocketThread that a connection handler has finished the client request
     * and closed the socket.
     * @param handler
     */
    void notifyDisconnect(ConnectionHandler handler) {
        if (datahandlers[handler.getId()] == handler) {
            datahandlers[handler.getId()] = null;
        }
    }

    /**
     * This will return the servermanager attached to this socketthread
     * @return
     */
    MarleManager getServermanager() {
        return servermanager;
    }
}
