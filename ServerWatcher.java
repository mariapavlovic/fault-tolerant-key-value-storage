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


public class ServerWatcher implements CuratorWatcher {
    
	CuratorFramework curClient;
	KeyValueHandler ServiceHandler;
    String zkNode;

	String IP;
	Integer Port;

	// client constructor that is called in main()
    ServerWatcher(CuratorFramework curClient, KeyValueHandler ServiceHandler, String zkNode, String IP, Integer Port) {
		this.curClient = curClient; 
		this.ServiceHandler = ServiceHandler;
		this.zkNode = zkNode;
		this.IP = IP;
		this.Port = Port;
    }

    // method for self re-identification (who am I??) upong the occurence of the Watched Event (addition/deletion of a znode)
	void identify() throws Exception {
		State current_state = ServiceHandler.getState();
		curClient.sync();
		List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);			// this sets the watch event -> will trigger whenever a znode is added/deleted

		if (children.size() == 1){										// ALONE
			// System.out.println("WATCHER - IDENTIFY: children size = 1");
			ServiceHandler.setState(State.PRIMARY);
			ServiceHandler.setIsAlone(true);
		} else {														// NOT ALONE
			Collections.sort(children);

			if (current_state == State.PRIMARY){
					// ServiceHandler.setIsAlone(false);
			} else if (current_state == State.BACKUP){
				
				/*
				* if (we are the head)
				* 		set to primary
				* 		set_isAlone(false)
				* else [if we are not the primary]
				* 		ping primary
				* 		if (response)
				* 			nothing
				* 		else [if no response]
				* 			become primary
				* 			if (SPARE exists [num_children == 3]) 
				* 			set not alone
				* 		close connections
				*/


				// extract the IP and Port# of the Head
				byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
				String strData = new String(data);
				String[] primary = strData.split(":");

				if (IP.equals(primary[0]) && Port.equals(Integer.parseInt(primary[1]))){
					// System.out.println("WATCHER: BACKUP - WE ARE THE HEAD");
					// we are the Head
					ServiceHandler.setState(State.PRIMARY);
					// ServiceHandler.setIsAlone(false);
				} else {
					// System.out.println("WATCHER: BACKUP - WE ARE NOT THE HEAD");
					// we are not the head
					KeyValueService.Client connectionPRIMARY = getConnection(children.get(0));

					try {
						connectionPRIMARY.ping();
					} catch (TException e) {
						ServiceHandler.setState(State.PRIMARY);
						if (children.size() < 3){
							ServiceHandler.setIsAlone(true);
						}
					}
					connectionPRIMARY.getOutputProtocol().getTransport().close();
				}
			} else if (current_state == State.SPARE) {

				/* if (num_children == 2)
				* 		ping primary
				* 		if (response == true)
				* 			set ourselves as BACKUP
				* 			request data
				*		else if (response == false)			<-- i.e. we sent to ourselves since PRIMARY with same IP:Port# crashed
				*			set ourselves as PRIMARY
				*			set is_alone = true
				*		else [if no response]
				*			set ourselves as PRIMARY
				*			set is_alone = true
				*
				* else [if (num_children == 3)]
				* 		ping A and B
				* 		if (A responds)
				*			if (false)
				*				set oursevles as backup
				*				request Data from B
				*		else [if A doesn't respond]
				*				set oursevles as backup
				*				request Data from B
				*		
				*		if (B responds)
				*			if (false)
				*				set oursevles as backup
				*				request Data from A
				*		else [if B doesn't respond]
				*			set oursevles as backup
				*				request Data from A
				* 
				*close connections
				*/

				KeyValueService.Client connectionPRIMARY = getConnection(children.get(0));
				KeyValueService.Client connectionBACKUP = getConnection(children.get(1));
				byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
				String strData = new String(data);
				String[] primary = strData.split(":");
				
				if (children.size() == 2){
					// PING A
					try {
						if (connectionPRIMARY.ping()){
							ServiceHandler.setState(State.BACKUP);
							connectionPRIMARY.completeDataTransfer(IP, Port);
						} else {
							ServiceHandler.setState(State.PRIMARY);
							ServiceHandler.setIsAlone(true);
						}
					} catch (TException e) {
						ServiceHandler.setState(State.PRIMARY);
						ServiceHandler.setIsAlone(true);
					}
				} else {	// if (children.size() == 3)
					// PING A
					try {
						if (!connectionPRIMARY.ping()){
							ServiceHandler.setState(State.BACKUP);
							connectionBACKUP.completeDataTransfer(IP, Port);
						}
					} catch (TException e) {
						ServiceHandler.setState(State.BACKUP);
						connectionBACKUP.completeDataTransfer(IP, Port);
					}

					// PING B
					try {
						if (!connectionBACKUP.ping()){
							ServiceHandler.setState(State.BACKUP);
							connectionPRIMARY.completeDataTransfer(IP, Port);
						}
					} catch (TException e) {
						ServiceHandler.setState(State.BACKUP);
						connectionPRIMARY.completeDataTransfer(IP, Port);
					}
				}

				connectionPRIMARY.getOutputProtocol().getTransport().close();
				connectionBACKUP.getOutputProtocol().getTransport().close();
			}
		}
	}


	KeyValueService.Client getConnection(String child) throws Exception{
		byte[] data = curClient.getData().forPath(zkNode + "/" + child);
		String strData = new String(data);
		String[] primary = strData.split(":");							// primary[0] = IP, primary[1] = port #			

		TSocket sock = new TSocket(primary[0], Integer.parseInt(primary[1]));
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		KeyValueService.Client client = new KeyValueService.Client(protocol);
		transport.open();

		return client;
	}
	
	synchronized public void process(WatchedEvent event) {				// the Watched Event is the addition/deletion of a znode from the children of the zknode [/lsilobad/ OR /m3pavlov/]
		try {
			identify();
		} catch (Exception e) {
			System.out.println("WATCHER: identify() failed");
		}
	}
    
}


