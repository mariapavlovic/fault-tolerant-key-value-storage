import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import java.util.concurrent.Semaphore;

public class Connection {
    TTransport transport;
    KeyValueService.Client client;

    public Connection(String host, int port) throws org.apache.thrift.TException {
        TSocket sock = new TSocket(host, port);
        this.transport = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(transport);
        transport.open();
        this.client = new KeyValueService.Client(protocol);
    }
}
