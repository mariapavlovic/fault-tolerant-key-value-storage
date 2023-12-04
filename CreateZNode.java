import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

// This is a Java program that creates a ZooKeeper node using the Apache Curator library
public class CreateZNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();							// configures the basic logger for the program
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 2) {														// checks if the number of command-line arguments is not equal to 2
			System.err.println("Usage: java CreateZKNode zkconnectstring zknode");	// zknode will be /$USER (?)
			System.exit(-1);
		}

		CuratorFramework curClient =												// create/initialize a CuratorFramework
			CuratorFrameworkFactory.builder()
			.connectString(args[0])													// Set the url to the zookeeper server (in our case, $ZKSTRING)
			.retryPolicy(new RetryNTimes(10, 1000))									// Retry 10 times, wait 1s between each retry.
			.connectionTimeoutMs(1000)												// Set connection timeout to 1s
			.sessionTimeoutMs(10000)												// Terminates the session if zookeeper does not hear from the client for more than 10s
			.build();
		curClient.start();															// starts the Curator client
		

		Runtime.getRuntime().addShutdownHook(new Thread() {							// shutdown hook is a thread that gets executed when the JVM (Java Virtual Machine) is shutting down
			public void run() {
				curClient.close();													// the code calls the close() method on the curClient object, which is an instance of CuratorFramework, to gracefully close the connection to ZooKeeper before the application exits
			}
		});

		ZKPaths.mkdirs(curClient.getZookeeperClient().getZooKeeper(), args[1]);		// responsible for creating a znode (ZooKeeper node) in the ZooKeeper cluster using the Curator framework.
		
		curClient.close();															// gracefully close the ZooKeeper client connection, and release the allocated resources
    }
}
