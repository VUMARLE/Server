package nl.vu.ict4d.marle.server.file;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import nl.vu.ict4d.marle.server.util.Utilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author RMH
 */
public class FileMeta {

    /** Version indentifier to know how to convert old meta types. */
    private static final short META_VERSION = 1;
    public static final String JSON_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private UUID id; // UUID
    private String name; // Original name of the file
    private String desc; // 
    private UUID sender; // GUID 
    private String senderip; // 255.255.255.255
    private Date date; // date (dd-MM-yyyy hh:mm)
    private byte[] checksum; // MD5 Hash
    private String location;
    private List<UUID> serverlocations;

    public FileMeta(UUID id, String name, String desc, UUID sender, String senderip,
            Date date, byte[] Checksum, String location, List<UUID> serverlocations) {
        this.id = id;
        this.name = name;
        this.desc = desc;
        this.sender = sender;
        this.senderip = senderip;
        this.date = date;
        this.checksum = Checksum;
        this.location = location;
        this.serverlocations = serverlocations;
    }

    /**
     * @return the id
     */
    public UUID getId() {
        return id;
    }
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the desc
     */
    public String getDesc() {
        return desc;
    }

    /**
     * @param desc the desc to set
     */
    public void setDesc(String desc) {
        this.desc = desc;
    }

    /**
     * @return the sender
     */
    public UUID getSender() {
        return sender;
    }

    /**
     * @param sender the sender to set
     */
    public void setSender(UUID sender) {
        this.sender = sender;
    }

    /**
     * @return the ip
     */
    public String getSenderIP() {
        return senderip;
    }

    /**
     * @param ip the ip to set
     */
    public void setSenderIp(String ip) {
        this.senderip = ip;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @return the Checksum
     */
    public byte[] getChecksum() {
        return checksum;
    }

    /**
     * @param checksum the Checksum to set
     */
    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }

    /**
     * @return the location
     */
    public String getLocation() {
        return location;
    }

    /**
     * @param location the location to set
     */
    public void setLocation(String location) {
        this.location = location;
    }

    public List<UUID> getServerlocations() {
        return serverlocations;
    }

    public void setServerlocations(List<UUID> serverlocations) {
        this.serverlocations = serverlocations;
    }

    /**
     * Creates a new JSONObject with the data from the filemeta object
     * @return JSONObject with the information from the object.
     */
    public JSONObject toJSON() {
        JSONObject object = new JSONObject();

        object.put("v", META_VERSION);

        object.put("id", getId().toString());
        object.put("name", getName());
        object.put("desc", getDesc());
        object.put("sender", getSender().toString());
        object.put("senderip", getSenderIP());
        // Set the date to specific format
        SimpleDateFormat format = new SimpleDateFormat(JSON_DATE_FORMAT);
        object.put("date", format.format(getDate()));
        object.put("checksum", Utilities.md5HashByteToString(getChecksum()));
        object.put("loc", getLocation());

        JSONArray serverlocs = new JSONArray();
        for (UUID srvloc : getServerlocations()) {
            serverlocs.add(srvloc.toString());
        }
        object.put("srvrlocs", serverlocs);

        return object;
    }

    /**
     * Creates a new FileMeta object from the contents of the JSONObject
     * @param object the JSONObject containing the information for the FileMeta
     * @return FileMeta object with details from the given JSONObject
     */
    public static FileMeta fromJSON(JSONObject object) throws ParseException {
        // Simple check if there is actually data
        if (object == null || !object.containsKey("id")) {
            return null;
        }

        // Version number if old values changed
        //short version = Short.parseShort(object.get("v").toString());

        UUID id = UUID.fromString(object.get("id").toString());
        String name = object.get("name").toString();
        String desc = object.get("desc").toString();
        UUID sender = UUID.fromString(object.get("sender").toString());
        String senderip = object.get("senderip").toString();

        // Format bak the date
        String dateString = object.get("date").toString();
        Date date = new SimpleDateFormat(JSON_DATE_FORMAT).parse(dateString);

        byte[] checksum = Utilities.md5HashStringToByte(object.get("checksum").toString());
        String loc = object.get("loc").toString();

        // Convert back the JSONArray
        JSONArray jsonLocs = (JSONArray) object.get("srvrlocs");
        List<UUID> serverloc = new LinkedList<UUID>();
        for (int i = 0; i < jsonLocs.size(); i++) {
            serverloc.add(UUID.fromString(jsonLocs.get(i).toString()));
        }

        // Create the object
        return new FileMeta(id, name, desc, sender, senderip, date, checksum, loc, serverloc);
    }
}
