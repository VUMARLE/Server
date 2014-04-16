package nl.vu.ict4d.marle.server.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 *
 * @author RMH
 */
public final class Utilities {

    public static final int DIGEST_BUFFER_SIZE = 1024;

    private Utilities() {
    }

    /**
     * This will create a byte array from a long value
     *
     * @param value value to convert to bytes
     * @return bytearray with the value of the long.
     */
    public static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /**
     * Converts a byte array back to a long
     *
     * @param buf value with the byte values for the long (must be 8 long!)
     * @return the long value of the bytes in the given array. This method will
     * return Long.MIN_VALUE if the buffer length is not equal to 8.
     */
    public static long bytesToLong(byte[] buf) {
        if (buf == null || buf.length != 8) {
            return Long.MIN_VALUE;
        }
        return ByteBuffer.wrap(buf).getLong();
    }

    /**
     * Creates an MD5 byte array from the given MD5 hash.
     *
     * @param hash the hash to convert back to bytes
     * @return String value of the given bytes
     */
    public static byte[] md5HashStringToByte(String hash) {
        byte[] buf = new byte[16];
        int i = 0;
        while (hash.length() > 0) {
            buf[i++] = (byte) Integer.parseInt(hash.substring(0, 2), 16);
            hash = hash.substring(2);
        }
        return buf;
    }

    /**
     * Creates an MD5 string from the given bytes.
     *
     * @param bytes
     * @return String value of the given bytes
     */
    public static String md5HashByteToString(byte[] bytes) {
        String result = "";
        for (int i = 0; i < bytes.length; i++) {
            result += Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1);
        }
        return result;
    }

    /**
     * This method will check if the two given byte arrays are the same.
     *
     * @param hash1 - The first value to check (can be null)
     * @param hash2 - The second value to check (can be null)
     * @return The method will return true if, and only if, both arrays are
     * exactly the same. If either of the hash values is NULL it will return false.
     */
    public static boolean validateHash(byte[] hash1, byte[] hash2) {
        if (hash1 != null && hash2 != null && hash1.length == hash2.length) {
            for (int i = 0; i < hash1.length; i++) {
                if (hash1[i] != hash2[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * This method will create a String checksum for the given file
     *
     * @param file - The file to create a checksum for
     * @return The md5 checksum of the gile in string format
     * @throws Exception
     */
    public static String createChecksumString(File file) throws Exception {
        return md5HashByteToString(createChecksum(file));
    }

    /**
     * This method will create a String checksum for the given stream.
     *
     * @param stream - The stream to create a checksum for. Note that the stream
     * will be read to EOF and <u>not</u> reset to a starting position.
     * @return The md5 checksum of the gile in string format
     * @throws Exception
     */
    public static String createChecksumString(InputStream stream) throws Exception {
        return md5HashByteToString(createChecksum(stream));
    }

    /**
     * This will open a stream to the given file and calculate the checksum of
     * it.
     *
     * @param file the file to calculate the checksum of.
     * @return byte array with the checksum
     * @throws Exception
     */
    public static byte[] createChecksum(File file) throws IOException {
        InputStream stream = new FileInputStream(file);
        byte[] checksum = createChecksum(stream);
        try {
            stream.close();
        } catch (IOException ex) {
            // ignore
        }
        return checksum;
    }

    /**
     * This method will create an MD5 checksum from the data on the given
     * stream. Note that the stream will be read to EOFand <u>not</u> reset to a
     * starting position.
     *
     * @param stream
     * @return byte array with the checksum
     * @throws IOException
     */
    public static byte[] createChecksum(InputStream stream) throws IOException {
        try {
            MessageDigest md5Hasher = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[DIGEST_BUFFER_SIZE];

            // Read the entire file into the digest
            int numRead;
            while ((numRead = stream.read(buffer)) != -1) {
                md5Hasher.update(buffer, 0, numRead);
            }

            // Do not close the stream here
            //stream.close();
            return md5Hasher.digest();
        } catch (NoSuchAlgorithmException ex) {
            // Elevate it to a runtime exception 
            // E.a. this should never be able to happen
            throw new RuntimeException("Could not calculate file checksum!");
        }
    }
}
