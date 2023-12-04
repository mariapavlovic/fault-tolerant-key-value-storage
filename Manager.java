import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

/* 
 * The purpose of this class is to maintain Thrift Client connections from the PRIMARY to the BACKUP
 * 
 * ... this is so that we don't need to make new Client connections every time we want to forward something
 * from PRIMARY -> BACKUP; instead we reuse existing ones because creating Thrift connections is kind of slow
 * 
 * This class does this by using static methods: i.e. the methods and objects don't belong to an instance
 * of the class, rather to the class itself.s
 */

public class Manager {
    private static List<Connection> connections = new ArrayList<>();

    private static String destIP;
    private static int destPort;

    // method to initialize/create THRIFT client connections                        <-- used when we establish a new BACKUP
    public static void initializeConnections(String IP, int port){
        // close existing connections before creating new ones
        closeConnections();

        destIP = IP;
        destPort = port;

        // System.out.println("MANAGER: initializeConnections to IP: " + IP + " Port#: " + port + "for the data transfer");
        // just make one initial THRIFT client connection
        for (int i = 0; i < 20; i++) {
            try {
                connections.add(new Connection(IP, port));
            } catch (Exception e) {
                System.out.println("MANAGER: initializeConnections failed to create connection, connections.size = " + connections.size());
            }
        }
        // System.out.println("MANAGER: initializeConnections completed, connections.size = " + connections.size());
    }

    // method to close existing connections
    private static void closeConnections() {
        synchronized (connections) {
            for (Connection c : connections) {
                try {
                    c.transport.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            connections.clear();
        }
    }

    // method to add a Thrift Connection back to the pool
    public static void addConnection(Connection c) {
        synchronized (connections) {   
            connections.add(c);                                 // By using the synchronized block, the method ensures that both threads properly serialize their access to the connections list
            connections.notify();
        }
    }

    // method to pop an available Connection from a specified node
    // method to pop an available Connection from a specified node
    public static Connection popConnection() throws org.apache.thrift.TException {
        Connection result = null;
        synchronized (connections) {
            if (!connections.isEmpty()) {
                // If there are available connections, remove one from the list
                result = connections.remove(0);
            } else {
                // If the list is empty, create a new connection and return it
                result = new Connection(destIP, destPort);  // will throw exception up if fails, we want this though
                
            }
        }
        return result;
    }
}
