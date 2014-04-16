package nl.vu.ict4d.marle.server.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;
import nl.vu.ict4d.marle.server.file.FileMeta;
import nl.vu.ict4d.marle.server.util.Utilities;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author RMH
 */
public class ClientSocket {

    private static final Logger logger = Logger.getLogger("MarleLogger");
    private static final int SERVER_DATA_SOCKET = 12333;
    private static final int BUFFER_SIZE = 16000;
    private final boolean isServer;
    private Socket socket;

    /**
     * Creates a new client socket in CLIENT connection mode
     */
    public ClientSocket() {
        this(false);
    }

    /**
     * This creates an itemsocket where the source (the program that created it) can be set to act
     * as a server node.
     * @param isServer - TRUE if the source is a SERVER node
     */
    public ClientSocket(boolean isServer) {
        this.isServer = isServer;
    }

    /**
     * Opens the connection to the given server.
     *
     * @param host
     * @return
     * @throws IOException
     */
    public boolean openConnection(InetAddress host) throws IOException {
        if (socket != null) {
            throw new IllegalStateException("Object already connected to a server!");
        }

        logger.debug("Connecting to " + host.getHostAddress());
        socket = new Socket(host, SERVER_DATA_SOCKET);

        // Read the response
        byte[] buffer = new byte[20];
        socket.getInputStream().read(buffer);
        String responseMessage = new String(buffer).trim();

        // Check if the connection got accepted
        if (responseMessage.equals(ConnectionMessages.SERVER_RESPONSE_ACCEPT)) {
            logger.debug("Server connection accepted!");
            
            // Write if this is a server or client
            socket.getOutputStream().write(isServer ? 1 : 0);
            socket.getOutputStream().flush();
            return true;
        } else {
            logger.debug("Server connection rejected!");
            // Rejected, close
            closeConnection();
            return false;
        }
    }

    /**
     * Closes the connection to the connected server
     */
    public void closeConnection() throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        try {
            socket.close();
        } finally {
            socket = null;
        }
    }

    /**
     * This method will request a file from the connected server
     *
     * @param fileID
     * @param stream
     * @throws IOException
     */
    public JSONArray requestMeta() throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        logger.debug("Requesting meta file ");
        String message = ConnectionMessages.SERVER_META_DOWNLOAD;
        // Write message
        socket.getOutputStream().write(message.getBytes());
        socket.getOutputStream().flush();

        int response = socket.getInputStream().read();

        if (response == ConnectionMessages.SERVER_FILEPULL_SENDING) {
            logger.debug(" Getting meta file");

            // Get the total file size first
            byte[] buffer = new byte[8];
            socket.getInputStream().read(buffer);

            // TODO: Fix so we can read larger files..
            long filesize = Math.min(Utilities.bytesToLong(buffer), (long) Integer.MAX_VALUE);
            logger.debug(" Filesize: " + filesize);

            // Resize buffer
            buffer = new byte[BUFFER_SIZE];
            int length, readBytes;

            OutputStream stream = null;
            ByteArrayOutputStream baos = null;
            try {
                baos = new ByteArrayOutputStream((int) filesize);
                stream = new BufferedOutputStream(baos);

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

                try {
                    JSONArray contentFile = (JSONArray) new JSONParser().parse(new String(baos.toByteArray()));
                    logger.debug(" File transfer complete!");
                    return contentFile;
                } catch (ParseException pe) {
                    logger.fatal("Could not decode the filemeta object!");
                    return null;
                }
            } finally {
                try {
                    if (stream != null)
                        stream.close();
                } catch (IOException ex) {
                }
                try {
                    if (baos != null)
                        baos.close();
                } catch (IOException ex) {
                }
            }
        } else {
            logger.error("Server gave an invalid response...!");
            return null;
        }
    }

    /**
     * This method will request a file from the connected server
     *
     * @param fileID
     * @param stream
     * @throws IOException
     */
    public boolean requestFile(UUID fileID, OutputStream stream) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        logger.debug("Requesting new file: " + fileID);
        String message = ConnectionMessages.SERVER_FILE_PULL;
        // Write message
        socket.getOutputStream().write(message.getBytes());
        socket.getOutputStream().flush();
        // Write the UUID
        socket.getOutputStream().write(fileID.toString().getBytes());
        socket.getOutputStream().flush();

        int response = socket.getInputStream().read();

        if (response == ConnectionMessages.SERVER_FILEPULL_SENDING) {
            logger.debug(" File was found");

            // Get the total file size first
            byte[] buffer = new byte[8];
            socket.getInputStream().read(buffer);

            long filesize = Utilities.bytesToLong(buffer);
            logger.debug(" Filesize: " + filesize);

            // Resize buffer
            buffer = new byte[BUFFER_SIZE];
            int length, readBytes;

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
            logger.debug(" File transfer complete!");
            return true;
        } else if (response == ConnectionMessages.SERVER_FILEPULL_MISSING) {
            logger.debug(" File was not found");
        } else {
            logger.error("Server gave an invalid response...!");
        }
        return false;
    }

    /**
     * This method will push a file to the connected server.
     *
     * @param meta details about the file
     * @return true if the file transfer was completed, or the connected server
     * already had the file (checksums validated).
     * @throws IOException
     *
     * @TODO remove the manager dependency here..
     */
    public boolean sendFile(FileMeta meta, File file) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        logger.debug("Pushing new file: ");
        socket.getOutputStream().write(ConnectionMessages.SERVER_FILE_PUSH.getBytes());
        socket.getOutputStream().flush();

        // Push the meta (size (as long) - date itself)
        byte[] jsonobj = meta.toJSON().toJSONString().getBytes();
        byte[] objsizebytes = Utilities.longToBytes((long) jsonobj.length);

        socket.getOutputStream().write(objsizebytes);
        socket.getOutputStream().flush();
        socket.getOutputStream().write(jsonobj);
        socket.getOutputStream().flush();

        // Clear buffers
        jsonobj = null;
        objsizebytes = null;

        int response = socket.getInputStream().read();

        if (response == ConnectionMessages.SERVER_FILEPUSH_ACCEPT) {
            logger.debug(" File was accepted");

            OutputStream output = socket.getOutputStream();

            // Send the filesize
            long filesize = file.length();
            logger.debug(" bytes to transfer: " + filesize);
            // Write the filesize
            output.write(Utilities.longToBytes(filesize));
            output.flush();

            // Prepare databuff
            byte[] bytearray = new byte[BUFFER_SIZE];

            FileInputStream fin = null;
            BufferedInputStream bin = null;
            try {
                // load the file into the databuffer array
                fin = new FileInputStream(file);
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
            logger.debug(" File transfer complete!");
            return true;
        } else if (response == ConnectionMessages.SERVER_FILEPUSH_DUPELICATE) {
            logger.debug(" File is a duplicate of an existing file!");
            return true;
        } else if (response == ConnectionMessages.SERVER_FILEPUSH_NOFREESPACE) {
            logger.debug(" Server has no free space");
        } else {
            logger.error("Server gave an invalid response...!");
        }
        return false;
    }

    /**
     * Sends a request to the connected server to delete the given file
     * @param fileID
     * @return
     * @throws IOException
     */
    public boolean deleteFile(UUID fileID) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        logger.debug("Deleting file");
        socket.getOutputStream().write(ConnectionMessages.SERVER_FILE_DELETE.getBytes());
        socket.getOutputStream().flush();

        // Send the ID to be deleted
        socket.getOutputStream().write(fileID.toString().getBytes());
        socket.getOutputStream().flush();

        int response = socket.getInputStream().read();

        if (response == ConnectionMessages.SERVER_FILEDELETE_DELETED) {
            logger.debug("File was deleted on server!");
            return true;
        } else if (response == ConnectionMessages.SERVER_FILEDELETE_MISSING) {
            logger.debug("Server could not find the file");
        } else {
            logger.error("Server gave an invalid response...!");
        }
        return false;
    }

    /**
     * Sends a meta update to the given servernode
     * @param meta
     * @throws IOException
     */
    public void sendMetaUpdate(FileMeta meta) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("No open connection!");
        }

        logger.debug("Updating meta file ");
        byte[] buffer = ConnectionMessages.SERVER_META_UPDATE.getBytes();
        // Write message
        socket.getOutputStream().write(buffer);
        socket.getOutputStream().flush();

        buffer = meta.toJSON().toJSONString().getBytes();

        // Write the filesize
        logger.debug(" Filesize: " + buffer.length);
        socket.getOutputStream().write(Utilities.longToBytes((long) buffer.length));
        socket.getOutputStream().flush();

        // Write the metefile
        socket.getOutputStream().write(buffer);
        socket.getOutputStream().flush();

        logger.debug(" Meta file update send to server ");
    }
}
