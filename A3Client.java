import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

import ca.uwaterloo.watca.ExecutionLogger;


// $JAVA_HOME/bin/java A3Client $ZKSTRING /$USER 4 10 1000
// -> manta address
// -> $USER
// -> 4 threads
// -> 10 seconds
// -> keys drawn from a set of 1000


// keyword implements is used to indicate that a class is adopting a particular interface.
// An interface in Java defines a contract or a set of methods that a class implementing that interface must implement
public class A3Client implements CuratorWatcher {
    static Logger log;
    
    String zkConnectString;
    String zkNode;
    int numThreads;
    int numSeconds;
    int keySpaceSize;
    CuratorFramework curClient;
    volatile boolean done = false;
    AtomicInteger globalNumOps;
    volatile InetSocketAddress primaryAddress;
    ExecutionLogger exlog;

    public static void main(String [] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: java A3Client zkconnectstring zknode num_threads num_seconds keyspace_size");
			System.exit(-1);
		}

		BasicConfigurator.configure();
		log = Logger.getLogger(A3Client.class.getName());

		A3Client client = new A3Client(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));

		try {
			client.start();							// this is a method defined in this class -> This initializes the connection to ZooKeeper (currator) and starts the execution logger
			client.execute();						// this is a method defined in this class -> This is where the main logic of the program resides. It performs the key-value operations and measures performance metrics.
		} catch (Exception e) {
			log.error("Uncaught exception", e);
		} finally {
			client.stop();							// this is a method defined in this class -> investigate
		}
    }

	// client constructor that is called in main()
    A3Client(String zkConnectString, String zkNode, int numThreads, int numSeconds, int keySpaceSize) {
		this.zkConnectString = zkConnectString; 
		this.zkNode = zkNode;
		this.numThreads = numThreads;
		this.numSeconds = numSeconds;
		this.keySpaceSize = keySpaceSize;
		globalNumOps = new AtomicInteger();
		primaryAddress = null;
		exlog = new ExecutionLogger("execution.log");
    }

    void start() {
		curClient =
			CuratorFrameworkFactory.builder()
			.connectString(zkConnectString)
			.retryPolicy(new RetryNTimes(10, 1000))
			.connectionTimeoutMs(1000)
			.sessionTimeoutMs(10000)
			.build();

		curClient.start();
		exlog.start();
    }

    void execute() throws Exception {
		primaryAddress = getPrimary();														// calls the getPrimary method to obtain the socket address of the Primary Server in the "Key-Value Service" 
		List<Thread> tlist = new ArrayList<>();												// a list of Threads
		List<MyRunnable> rlist = new ArrayList<>();											// a list of the Threads' corresponding Runnables -> a class that we defined below
		for (int i = 0; i < numThreads; i++) {												// 'numThreads' is passed as a parameter into the program
			MyRunnable r = new MyRunnable();
			Thread t = new Thread(r);
			tlist.add(t);
			rlist.add(r);
		}
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numThreads; i++) {												// launch each of the threads
			tlist.get(i).start();
		}
		log.info("Done starting " + numThreads + " threads...");
		System.out.println("Done starting " + numThreads + " threads...");
		Thread.sleep(numSeconds * 1000);													// sleep ourselves for numSeconds
		done = true;																		// this should indicate to the threads (executing their runnables) that it is time to finish their jobs
		for (Thread t: tlist) {
			t.join(1000);																	// Joins each thread: This waits for each thread to complete or for a maximum of 1 second
		long estimatedTime = System.currentTimeMillis() - startTime;
		int tput = (int)(1000f * globalNumOps.get() / estimatedTime);						// calculate the throughput
		System.out.println("Aggregate throughput: " + tput + " RPCs/s");
		long totalLatency = 0;
		for (MyRunnable r: rlist) {
			totalLatency += r.getTotalTime();												// runable objects contain a variable that containts the amount of time that it took to execute
		}
		double avgLatency = (double)totalLatency / globalNumOps.get() / 1000;
		System.out.println("Average latency: " + ((int)(avgLatency*100))/100f + " ms");
		}
	}

    void stop() {
		curClient.close();
		exlog.stop();
    }

    InetSocketAddress getPrimary() throws Exception {											// method is declared to throw an Exception
		while (true) {																			// continuously attempt to retrieve the primary server's address until success or exception
			curClient.sync();																	// synchronizes the client's view of the ZooKeeper znode with the actual state in the ZooKeeper ensemble. This ensures that the latest updates are reflected in the client's view.
			List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode); // retrieves the list of children nodes under the specified zkNode [forPath(zkNode)]
			if (children.size() == 0) {
				log.error("No primary found");
				Thread.sleep(100);																// i.e. no Primary Server found -> delay
				continue;
			}
			Collections.sort(children);															// sort the children lexicographically
			byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));			// The data of the first child node (which represents the primary server's address) is retrieved -> Q: Why is this the primary replica?
			String strData = new String(data);
			String[] primary = strData.split(":");
			log.info("Found primary " + strData);
			return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));				// return the socket address of the primary server from ZooKeeper: IP, port #
		}
    }

    KeyValueService.Client getThriftClient() {
	while (true) {
	    try {
			TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
			TTransport transport = new TFramedTransport(sock);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
		return new KeyValueService.Client(protocol);
	    } catch (Exception e) {
			log.error("Unable to connect to primary");
	    }
	    try {
			Thread.sleep(100);
	    } catch (InterruptedException e) {}
	}
    }

    synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
			primaryAddress = getPrimary();
		} catch (Exception e) {
			log.error("Unable to determine primary");
		}		
    }

    class MyRunnable implements Runnable {
		long totalTime;
		KeyValueService.Client client;																// instance of KeyValueService.Client, which represents the client for interacting with the key-value service using the Thrift framework

		// constructor method that initializes the runnabl
		MyRunnable() throws TException {
			client = getThriftClient();
		}
		
		long getTotalTime() { return totalTime; }
		
		public void run() {
			Random rand = new Random();																// used for generating random values
			totalTime = 0;
			long tid = Thread.currentThread().getId();
			int numOps = 0;
			try {
				while (!done) {																		// execute the loop until the client asserts DONE
					long startTime = System.nanoTime();
					//log.info("Starting operation at : " + startTime);
					if (rand.nextBoolean()) {														// invoke a PUT operation on the Key-Value Service
						while (!done) {
							try {
							String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
							String value = "value-" + Math.abs(rand.nextLong());
							exlog.logWriteInvocation(tid, key, value);
							client.put(key, value);													// RPC CALL
							exlog.logWriteResponse(tid, key);
							numOps++;
							break;
							} catch (Exception e) {
							log.error("Exception during put");
							Thread.sleep(100);
							client = getThriftClient();
							}
						}
					} else {																		// invoke a GET operation on the Key-Value Service
						while (!done) {
							try {
							String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
							exlog.logReadInvocation(tid, key);
							String resp = client.get(key);											// RPC CALL
							exlog.logReadResponse(tid, key, resp);
							numOps++;
							break;
							} catch (Exception e) {
							log.error("Exception during get");
							Thread.sleep(100);
							client = getThriftClient();
							}
						}
					}
					long diffTime = System.nanoTime() - startTime;
					totalTime += diffTime / 1000;
				}
			} catch (Exception x) {
				x.printStackTrace();
			}	
			globalNumOps.addAndGet(numOps);
		}
    }
}
