package test_thrift_client;

import java.util.stream.IntStream;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import test_thrift_server.ThriftService;

public class ThriftClient {

  public static void main(String[] args) {
    IntStream.range(0, 100).parallel().forEach(i -> {
      try {
        TTransport transport;

        transport = new TSocket("localhost", 9090);
        transport = new TFramedTransport(transport);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftService.Client client = new ThriftService.Client(protocol);

        System.out.println(Integer.toString(i) + ":" + client.test(Integer.toString(i), "bbb"));

        transport.close();
      } catch (TTransportException e) {
        e.printStackTrace();
      } catch (TException x) {
        x.printStackTrace();
      }
    });
  }

}
