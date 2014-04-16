package nl.vu.ict4d.marle.server.data.sync;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Stack to list file that are marked for replication to other servers
 * @author RMH
 */
public class ReplicateStack {

    private static final Object _SYNCLOCK = new Object();
    private final List<UUID> items;

    public ReplicateStack() {
        items = new LinkedList<>();
    }

    /**
     * Add a file for replication to a different server
     * @param id
     */
    public void addFileForReplication(UUID id) {
        synchronized (_SYNCLOCK) {
            items.add(id);
        }
    }
    
    /**
     * Removes an item from the replication list
     * @param id 
     */
    public void removeReplicationFile(UUID id) {
        //TODO
    }

    /**
     * Creates a list of the items on the stack.
     * @param amount - The max amount of items to get.
     * @return list of fileid's that need to be replicated.
     */
    public List<UUID> getItems(int amount) {
        List<UUID> retList = new LinkedList<>();

        int i = amount;
        synchronized (_SYNCLOCK) { 
            // Get the first items from the list (FIFO)
            for (Iterator<UUID> it = items.iterator(); it.hasNext() && i > 0;) {
                UUID object = it.next();
                
                retList.add(object);
                
                // Remove the item from the stack
                it.remove();
                i--;
            }
        }

        return Collections.unmodifiableList(retList);
    }
    
    /**
     * This method will return the amount of items that are currently waiting to be replicated
     * @return 
     */
    public int getItemsOnStack() {
        synchronized (_SYNCLOCK) {
            return items.size();
        }
    }
}
