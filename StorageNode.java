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

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient =
			CuratorFrameworkFactory.builder()
			.connectString(args[2])
			.retryPolicy(new RetryNTimes(10, 1000))
			.connectionTimeoutMs(1000)
			.sessionTimeoutMs(10000)

			.build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
		});

		// this is TThreadPoolServer -> one thread to accept connections and then handles each connection using a dedicated thread drawn from a pool of worker threads
		KeyValueHandler ServiceHandler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(ServiceHandler);
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();													// blocking thread: this thread gets blocked... server is serving in the background though
			}
		}).start();

		String dataString = args[0] + ":" + args[1];							// dataString = String = IP:port#
		byte[] dataBytes = dataString.getBytes();

		curClient.create()
			.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
			.forPath(args[3] + "/znode", dataBytes);							// path = manta/lsilobad

		// set our current server status to SPARE
		ServiceHandler.setState(State.SPARE);

		ServerWatcher watcher = new ServerWatcher(curClient, ServiceHandler, args[3], args[0], Integer.parseInt(args[1]));

		// determine our actual status -> is it a PRIMARY? BACKUP? or SPARE?
		watcher.identify();														// NOTE: this call to identify also sets the watched event

		// System.out.println("STORAGENODE: Server is initialized as: " + ServiceHandler.getState() + " isAlone: " + ServiceHandler.get_isAlone());
    }
}
