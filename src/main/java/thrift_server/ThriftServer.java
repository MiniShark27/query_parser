package thrift_server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

/**
 * Server for thrift clients. This is what needs to be started to receive
 * requests via thrift
 */
public class ThriftServer {

  /**
   * Starts a "simple" (it wasn't for me to set up but ok...) thrift server on
   * port 9090
   */
  public static void startSimpleServer() {
    ThriftService.Processor<ThriftServiceHandler> processor = new ThriftService.Processor<>(new ThriftServiceHandler());
    try {
      TServerTransport serverTransport = new TServerSocket(9090);
      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
      System.out.println("Starting the simple server...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Runs the thrift server
   */
  public static void main(String[] args) {
    startSimpleServer();
  }
}