package thrift_client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import thrift_server.ThriftService;

public class ThriftClient {

  public static void main(String[] args) {

    try {
      TTransport transport;

      transport = new TSocket("localhost", 9090);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      ThriftService.Client client = new ThriftService.Client(protocol);

      System.out.println(client.test("aaa", "bbb"));

      transport.close();
    } catch (TTransportException e) {
      e.printStackTrace();
    } catch (TException x) {
      x.printStackTrace();
    }
  }

}
