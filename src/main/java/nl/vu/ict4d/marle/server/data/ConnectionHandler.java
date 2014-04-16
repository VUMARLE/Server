package nl.vu.ict4d.marle.server.data;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.NoSuchFileException;
import java.text.ParseException;
import java.util.Map;
import java.util.UUID;
import nl.vu.ict4d.marle.server.file.FileMeta;
import nl.vu.ict4d.marle.server.multicast.NodeSpotter;
import nl.vu.ict4d.marle.server.multicast.ServerNode;
import nl.vu.ict4d.marle.server.util.Utilities;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * A handler for data connections to the server
 *
 * @author RMH
 */
class ConnectionHandler implements Runnable {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    private static final int BUFFER_SIZE = 16000;
    private ServerSocketThread parent;
    private Thread self_thread;
    private Socket socket;
    private boolean connectedToServer = false;
    private int id;

    ConnectionHandler(int id, ServerSocketThread parent) {
        this.id = id;
        this.parent = parent;
    }

    /**
     * Gets the connection handler ID
     *
     * @return
     */
    public int getId() {
        return id;
    }

    /**
     * Makes the ConnectionHandler start handling the given connection
     */
    public void handle(Socket socket) {
        if (this.socket != null) {
            throw new IllegalArgumentException("This connectionhandler is already handling a connection!");
        }

        this.socket = socket;
        self_thread = new Thread(this);
        self_thread.start();
    }

    /**
     * Gets the hidden thread of this ConnectionHandler
     *
     * @return
     */
    public Thread getThread() {
        return self_thread;
    }

    @Override
    public void run() {
        try {
            // Check if we know if it is a server or client
            if (socket.getInputStream().read() == 1) {
                connectedToServer = true;
                logger.debug("Start handling new connection from: " + socket.getInetAddress().getHostAddress() + " (servermode)");
            } else {
                connectedToServer = false;
                logger.debug("Start handling new connection from: " + socket.getInetAddress().getHostAddress() + " (clientmode)");
            }

            // Handle stuff            
            byte[] buffer = new byte[8];
            int length;

            // Read the requests
            while ((length = socket.getInputStream().read(buffer)) != -1) {
                String requestString = new String(buffer, 0, length).trim();

                switch (requestString) {
                    case ConnectionMessages.SERVER_FILE_PULL:
                        handleFilePullRequest();
                        break;
                    case ConnectionMessages.SERVER_FILE_PUSH:
                        handleFilePushRequest();
                        break;
                    case ConnectionMessages.SERVER_FILE_DELETE:
                        handleFileDeleteRequest();
                        break;

                    case ConnectionMessages.SERVER_META_DOWNLOAD:
                        handleMetaDownload();
                        break;
                    case ConnectionMessages.SERVER_META_UPDATE:
                        handleMetaUpdate();
                        break;

                    default:
                        logger.error("Unknown request '" + requestString + "', closing connection.. ");
                        break;

                }
            }
            logger.debug("Handling finished, closing connection...");

        } catch (Exception ex) {
            // Special exception catcher, just to be sure
            logger.error("Handling the client connection went wrong. Outer exception handler reached!", ex);
        } finally {
            try {
                socket.close();
            } catch (IOException ex) {
                // ignore
            }
            parent.notifyDisconnect(this);
        }
    }

    /**
     * This is the handler for the request of the complete metafile collection
     */
    private void handleMetaDownload() throws IOException {
        logger.debug("Meta request");

        try {
            JSONArray contentfile = parent.getServermanager().getArchive().getContentFile();

            // First mark that we are sending the file
            socket.getOutputStream().write((byte) ConnectionMessages.SERVER_FILEPULL_SENDING);
            socket.getOutputStream().flush();

            // Push the meta (size (as long) - date itself)
            byte[] jsonobj = contentfile.toJSONString().getBytes();
            byte[] objsizebytes = Utilities.longToBytes((long) jsonobj.length);

            socket.getOutputStream().write(objsizebytes);
            socket.getOutputStream().flush();

            socket.getOutputStream().write(jsonobj);
            socket.getOutputStream().flush();

        } catch (org.json.simple.parser.ParseException ex) {
            logger.log(Priority.ERROR, "Could not load the content file for transfer!", ex);
            socket.getOutputStream().write((byte) ConnectionMessages.SERVER_ERROR);
            socket.getOutputStream().flush();
        }

    }

    /**
     * The handler for file requests. This method will send the requested file
     * back to the client
     *
     * @throws IOException
     */
    private void handleFilePullRequest() throws IOException {
        logger.debug("File pull request");

        OutputStream output = socket.getOutputStream();

        // Get the filename
        byte[] buffer = new byte[36];
        socket.getInputStream().read(buffer);
        UUID fileid = UUID.fromString(new String(buffer));

        try {
            parent.getServermanager().getArchive().checkIfExists(fileid);
        } catch (NoSuchFileException nsfe) {
            // Send response that server does not have that file
            logger.debug("File was not found.");

            // Mark that we do not know the file..
            output.write((byte) ConnectionMessages.SERVER_FILEPULL_MISSING);
            output.flush();

            logger.debug("File transfer aborted.");
            return;
        }

        // File exists!

        logger.debug("Sending file to client.");
        // First mark that we are sending the file
        output.write((byte) ConnectionMessages.SERVER_FILEPULL_SENDING);
        output.flush();

        long filesize = parent.getServermanager().getArchive().getFileSize(fileid);
        logger.debug(" bytes to transfer: " + filesize);
        // Write the filesize
        output.write(Utilities.longToBytes(filesize));
        output.flush();

        // Prepare databuff
        byte[] bytearray = new byte[BUFFER_SIZE];

        InputStream fin = null;
        BufferedInputStream bin = null;
        try {
            // load the file into the databuffer array
            fin = parent.getServermanager().getArchive().getFile(fileid);
            bin = new BufferedInputStream(fin);

            // Sending the data in chunks
            int length;
            while ((length = bin.read(bytearray)) != -1) {
                output.write(bytearray, 0, length);
                output.flush();
            }

            logger.debug("File transfer complete!");
        } finally {
            try {
                if (fin != null) {
                    fin.close();
                }
            } catch (IOException ex) {
            }
            try {
                if (bin != null) {
                    bin.close();
                }
            } catch (IOException ex) {
            }
        }
    }

    /**
     * This method will handle filepush requests send from its clients.
     * It will automatically link it up to the data store and such.
     *
     * @throws IOException
     */
    private void handleFilePushRequest() throws IOException {
        logger.debug("File push request");

        // Get the filemeta size
        byte[] buffer = new byte[8];
        socket.getInputStream().read(buffer);
        // Can metafiles be larger than INT.MAXVALUE?
        long filemetasize = Math.min(Utilities.bytesToLong(buffer), (long) Integer.MAX_VALUE);

        // Can be shrink this somehow?
        buffer = new byte[(int) filemetasize];
        socket.getInputStream().read(buffer);

        JSONObject obj = (JSONObject) JSONValue.parse(new String(buffer));
        OutputStream output = socket.getOutputStream();

        // Create the filemeta object!
        FileMeta meta;
        try {
            meta = FileMeta.fromJSON(obj);
        } catch (ParseException ex) {
            logger.fatal("Incorrect metadata send by client. Aborting..");
            output.write((byte) ConnectionMessages.SERVER_ERROR);
            return;
        }

        // TODO: Check in the data store here?
        File transferFile = parent.getServermanager().getArchive().getFileObject(meta.getId());

        buffer = null;
        // Calculate the checksum if the file already exists.
        if (transferFile.exists()) {
            try {
                // Reuse buffer to hold the checksum
                buffer = Utilities.createChecksum(transferFile);
            } catch (Exception ex) {
                logger.error("Could not calculate checksum, considering file invalid.");
            }
        }

        // No need to check for file exising here, buffer will be NULL so always invalid!
        if (Utilities.validateHash(buffer, meta.getChecksum())) {
            // Send response that server does not have that file
            logger.debug(" File already exists, aborting.");

            // Mark that we do not know the file..
            output.write((byte) ConnectionMessages.SERVER_FILEPUSH_DUPELICATE);
            output.flush();

            logger.debug("File transfer aborted.");
        } else {
            logger.debug("Sending file to client.");
            // First mark that we are sending the file
            output.write((byte) ConnectionMessages.SERVER_FILEPUSH_ACCEPT);
            output.flush();


            // Get the file size
            buffer = new byte[8];
            socket.getInputStream().read(buffer);
            long filesize = Utilities.bytesToLong(buffer);
            logger.debug(" bytes to transfer: " + filesize);

            // Resize buffer
            buffer = new byte[BUFFER_SIZE];
            int length, readBytes;

            ByteArrayOutputStream stream = null;
            try {
                //stream = new FileOutputStream(transferFile);

                // Currently we cannot handle this, disconnect!
                if (filesize > (long) Integer.MAX_VALUE) {
                    socket.close();
                    return;
                }

                stream = new ByteArrayOutputStream((int) filesize);

                // Loop till entire file received
                while (filesize > 0) {
                    length = (int) Math.min(filesize, (long) BUFFER_SIZE);

                    // Read/write the data
                    readBytes = socket.getInputStream().read(buffer, 0, length);

                    // Use the readBytes from hereon (this can differ!)
                    stream.write(buffer, 0, readBytes);
                    stream.flush();

                    filesize -= readBytes;
                }

                parent.getServermanager().getArchive().saveFile(meta.getId(), stream.toByteArray());

                // No sender, so new file from client
                if (meta.getSenderIP().isEmpty()) {
                    meta.setSenderIp(socket.getInetAddress().getHostAddress());
                } else {

                    // Replication push
                    if (meta.getServerlocations().contains(parent.getServermanager().getServerUUID())) {
                        // Save filemeta in archive
                        parent.getServermanager().getArchive().updateContentFile(meta);
                    } else {
                        // Add server to serverlocs of the meta file
                        meta.getServerlocations().add(parent.getServermanager().getServerUUID());

                        // Save filemeta in archive
                        parent.getServermanager().getArchive().updateContentFile(meta);

                        // Add file for replication (this will also push changes to other servers).
                        parent.getServermanager().getReplicationStack().addFileForReplication(meta.getId());
                        parent.getServermanager().getReplicator().interrupt();
                    }
                }

            } catch (IOException ex) {
                logger.fatal("Error in receiving file from client!", ex);
                // For a hard close (as the stream is now invalid!
                socket.close();
            } finally {
                if (stream != null)
                    try {
                        stream.close();
                    } catch (IOException ex) {
                        // ignore
                    }
            }
        }
    }

    /**
     * This will handle deletion requests
     */
    private void handleFileDeleteRequest() throws IOException {
        logger.debug("File delete request");
        try {
            OutputStream output = socket.getOutputStream();

            // Get the filename
            byte[] buffer = new byte[36];
            socket.getInputStream().read(buffer);
            UUID fileid = UUID.fromString(new String(buffer));

            try {
                // Check if we have meta for this file
                if (parent.getServermanager().getArchive().getFileMeta(fileid) == null) {
                    // There exists no meta for this file?
                    output.write(ConnectionMessages.SERVER_FILEDELETE_MISSING);
                    return;
                }
            } catch (IOException ex) {
                logger.error("Could not validate if the file exists! ", ex);
                output.write(ConnectionMessages.SERVER_ERROR);
                return;
            }

            // First remove it from replication (if new)
            parent.getServermanager().getReplicationStack().removeReplicationFile(fileid);

            // First remove it from our own meta
            try {
                parent.getServermanager().getArchive().removeMetaFromContentFile(fileid);
            } catch (org.json.simple.parser.ParseException ex) {
                logger.error("Could not update meta file (INVALID META!)");
            }

            // Cascade update to other servers if came from client
            if (!this.connectedToServer) {
                // Message from client, cascade to other servers
                ClientSocket srvsock = null;
                Map<InetAddress, ServerNode> nodes = new NodeSpotter().findNodes();
                for (InetAddress addr : nodes.keySet()) {
                    try {
                        srvsock = new ClientSocket(true);
                        srvsock.openConnection(addr);
                        srvsock.deleteFile(fileid);
                    } catch (IOException ex) {
                        logger.error("Failed to send metadelete to " + nodes.get(addr).getId());
                    } finally {
                        try {
                            if (srvsock != null)
                                srvsock.closeConnection();
                        } catch (IOException ex) {
                        }
                    }
                }
            }

            try {
                // Delete the file if it exists on this server
                parent.getServermanager().getArchive().checkIfExists(fileid);
                parent.getServermanager().getArchive().deleteFile(fileid.toString());
            } catch (NoSuchFileException ex) {
                // Ignore (make it cascade to other servers!)
            }

            // Write it was completed
            output.write(ConnectionMessages.SERVER_FILEDELETE_DELETED);

            logger.info("File '" + fileid.toString() + "' deleted by client!");
        } catch (IOException ex) {
            logger.fatal("Error in deleting file!", ex);
            // For a hard close (as the stream is now invalid!
            socket.close();
        }
    }

    private void handleMetaUpdate() {
        logger.debug("File meta update request");
        try {
            // Get the filemeta size
            byte[] buffer = new byte[8];
            socket.getInputStream().read(buffer);
            // Can metafiles be larger than INT.MAXVALUE?
            long filemetasize = Math.min(Utilities.bytesToLong(buffer), (long) Integer.MAX_VALUE);

            buffer = new byte[(int) filemetasize];
            socket.getInputStream().read(buffer);

            JSONObject obj = (JSONObject) JSONValue.parse(new String(buffer));

            // Create the filemeta object!
            FileMeta meta;
            try {
                meta = FileMeta.fromJSON(obj);
            } catch (ParseException ex) {
                logger.fatal("Incorrect metadata send by client. Aborting..");
                return;
            }

            // Update first in own library
            this.parent.getServermanager().getArchive().updateContentFile(meta);

            // Cascade update to other servers if came from client
            if (!this.connectedToServer) {
                // Message from client, cascade to other servers
                ClientSocket srvsock = null;
                Map<InetAddress, ServerNode> nodes = new NodeSpotter().findNodes();
                for (InetAddress addr : nodes.keySet()) {
                    try {
                        srvsock = new ClientSocket(true);
                        srvsock.openConnection(addr);
                        srvsock.sendMetaUpdate(meta);
                    } catch (IOException ex) {
                        logger.error("Failed to send metadelete to " + nodes.get(addr).getId());
                    } finally {
                        try {
                            if (srvsock != null)
                                srvsock.closeConnection();
                        } catch (IOException ex) {
                        }
                    }
                }
            }
        } catch (IOException ex) {
            logger.fatal("Error in updating metafile!", ex);
            // For a hard close (as the stream is now invalid!
        }
    }
}
