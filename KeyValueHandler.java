import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.w3c.dom.events.Event;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

// Libraries that we added (Maria i Luka)
import org.apache.thrift.async.*;

import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;


public class KeyValueHandler implements KeyValueService.Iface {
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private ConcurrentHashMap<String, String> myMap;
    private ConcurrentHashMap<String, Integer> requestSeqMap;
    private AtomicInteger requestSeq;

    private boolean isAlone;
    private Manager backupManager;
    private State state;

    // for debugging
    public boolean get_isAlone(){
        return this.isAlone;
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.myMap = new ConcurrentHashMap<String, String>();	
        this.requestSeqMap = new ConcurrentHashMap<String, Integer>();
        this.requestSeq = new AtomicInteger(0);
    }

    /*  
    * GET and SET functions for private variables belonging to each znode
    */
    public void setState(State s) {
        this.state = s;
    }

    public void setIsAlone(boolean b) {
        this.isAlone = b;
    }

    public State getState() {
        return this.state;
    }

    public String get(String key) throws org.apache.thrift.TException {	
        // from the slides:
        // Do not let a backup storage node answer get/put RPCs from the client 
        // since it may not have the latest data. Throw exceptions instead.
        if (state == State.SPARE ) {
            throw new org.apache.thrift.TException("Backup is not allowed to respond to GET operation.");
        }

        String ret = myMap.get(key);
        if (ret == null) {
            return "";
        } else {
            return ret;
        }
    }

    /* 
     * this function is from POV of primary, PUT request came in from client
     * primary updates its local map
     * primary also needs to forward PUT operation to backups
     */ 
    public void put(String key, String value) throws org.apache.thrift.TException {        
        if (!state.equals(State.SPARE)) {
            myMap.put(key, value); // first update local map of primary
            // forward the PUT operation to backup too, if there is one
            if (!isAlone) {
                forwardToBackup(key, value);
            }
        } else {
            // from the slides:
            // Do not let a backup storage node answer get/put RPCs from the client 
            // since it may not have the latest data. Throw exceptions instead.
            throw new org.apache.thrift.TException("Spare is not allowed to respond to GET operation.");
        }
    }

    private void forwardToBackup(String key, String value) throws org.apache.thrift.TException {
        Connection connectionToBackup = null;
        try {
            connectionToBackup = Manager.popConnection();
            connectionToBackup.client.forwardRequest(key, value, requestSeq.incrementAndGet());
            Manager.addConnection(connectionToBackup);
        } catch (Exception e) {
            this.isAlone = true;
            e.printStackTrace();
        }
    }

    /*
     * forwarding PUT request from primary to backups
     * this function is from POV of backup node, primary has called on it
     * we are updating backups local key-value map, and local sequence map
     */
    public void forwardRequest(String key, String value, int sequence) {
        // already wrote to this key in the hashmap
        if (requestSeqMap.containsKey(key)) {
            // only update map if the value is from a more recent PUT operation
            // (older writes would have gotten overwritten anyway so ignore them)
            if (sequence >= requestSeqMap.get(key)) {
                requestSeqMap.put(key, sequence);
                myMap.put(key, value);
            }
        } else {
            // never written to this key yet
            requestSeqMap.put(key, sequence);
            myMap.put(key, value);
        }
    }

    /*
     * RPC call to a znode, primary or backup to check that the znode is still alive
     * check state to cover cases of address & port reuse
     */
    public boolean ping() throws org.apache.thrift.TException {
        return (this.state != State.SPARE);
    }

    /* 
     * called when a NEW BACKUP is created, as we need to immediately do a COMPLETE data transfer
     * primary calls on this function, but within it reaches out to the backups
     * 
     * From the slides:
     * When initializing a backup storage node, copy over the entire data set from the primary 
     * as soon as possible. You can do this using one large RPC, or a collection of smaller RPCs. 
     * Do not wait for each key to be overwritten by a new put operation.
     */ 
    public void completeDataTransfer(String host, int port) throws org.apache.thrift.TException {
        // flush the Manager's thrift clients and make new ones (for the new backup)
        Manager.initializeConnections(host, port);
        setIsAlone(false);

        int sendLimit = 100000;
        List<String> keys = new ArrayList<String>(myMap.keySet());

        List<String> values = new ArrayList<String>(keys.size());
        for(int i = 0; i < keys.size(); i++) {
            values.add(i, myMap.get(keys.get(i)));
        }
        int dataToSend = keys.size();
        if (dataToSend <= sendLimit) {
            // send whole hash map in one go
            sendDataToBackup(keys, values);
        } else {
            int start = 0;
            int end = sendLimit;
            int remaining = dataToSend;
            while(end <= dataToSend) {
                sendDataToBackup(keys.subList(start, end), values.subList(start, end));
                remaining -= (end - start);
                start = end;
                if (remaining > sendLimit) {
                    end = start + sendLimit;
                } else {
                    if (remaining == 0){
                        break;
                    }
                    end = start + remaining;
                }
            }
        }
    }

    private void sendDataToBackup(List<String> keys, List<String> values) throws org.apache.thrift.TException {
        Connection connectionToBackup = null;
        try {
            connectionToBackup = Manager.popConnection();
            connectionToBackup.client.setMap(keys, values);
            Manager.addConnection(connectionToBackup);
        } catch (Exception e) {
            this.isAlone = true;
            e.printStackTrace();
        }
    }

    /*
     * this function is from POV of backup node, we are updating its local key-value map
     * its sequence map doesn't need to be set up here, since the key-value map is being initialized, so all sequences will be 0
     */
    public void setMap(List<String> keys, List<String> values) {
        for (int i = 0; i < keys.size(); i++) {
            if (!myMap.containsKey(keys.get(i))) {
                myMap.put(keys.get(i), values.get(i));
            }
        }
    }
}
