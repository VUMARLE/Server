package nl.vu.ict4d.marle.server.multicast;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Small class containing information about a server node
 */
public class ServerNode {

    private static final byte[] VALIDATE_BYTES = new byte[]{69, 70, 73};
    private UUID id;
    private long freespace;
    private int activeConnections;

    public ServerNode(UUID id, long freespace, int activeConnections) {
        this.id = id;
        this.freespace = freespace;
        this.activeConnections = activeConnections;
    }

    /**
     * The ID of the server node
     * @return
     */
    public UUID getId() {
        return id;
    }

    /**
     * The amount of space that is available on the server
     * @return
     */
    public long getFreespace() {
        return freespace;
    }

    /**
     * Upddates the amount of space that is on the servernode
     * @param freespace
     */
    public void setFreespace(long freespace) {
        this.freespace = freespace;
    }

    /**
     * The amount of connections the server is handling.
     * @return
     */
    public int getActiveConnections() {
        return activeConnections;
    }

    /**
     * Converts the servernode data to an array of <b>51</b> bytes containing all data.
     * @return byte array with the information
     */
    public byte[] toBytes() {
        ByteBuffer buff = ByteBuffer.allocate(3 + 36 + 8 + 4);

        buff.put(VALIDATE_BYTES);
        buff.put(id.toString().getBytes());
        buff.putLong(3 + 36, freespace);
        buff.putInt(3 + 36 + 8, activeConnections);

        return buff.array();
    }

    /**
     * Constructs a new ServerNode object from a 51 byte array.
     * @param val a byte array with information about the servernode
     * @return A servernode object with the values from the array. If the given val is null or the
     * size of the array is not <b>51</b> NULL will be returned.
     */
    public static ServerNode fromBytes(byte[] val) {
        if (val == null || val.length != 51) {
            return null;
        }
        ByteBuffer buff = ByteBuffer.wrap(val);

        // Validate the servernode obj
        byte[] validateBytes = new byte[VALIDATE_BYTES.length];
        buff.get(validateBytes);
        for (int i = 0; i < validateBytes.length; i++) {
            if (validateBytes[i] != VALIDATE_BYTES[i])
                return null;
        }

        byte[] uuidBuffer = new byte[36];
        buff.get(uuidBuffer);
        UUID id = UUID.fromString(new String(uuidBuffer));

        long freespace = buff.getLong(3 + 36);
        int activeConnections = buff.getInt(3 + 36 + 8);

        return new ServerNode(id, freespace, activeConnections);
    }
}
