package test_thrift_server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class ThriftServer {

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

  public static void startAsyncServer() {
    ThriftService.AsyncProcessor<ThriftServiceHandler> processor = new ThriftService.AsyncProcessor<>(
        new ThriftServiceHandler());
    try {
      TNonblockingServerTransport transport = new TNonblockingServerSocket(9090);
      TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(transport).selectorThreads(8)
          .workerThreads(10).acceptQueueSizePerThread(20).processor(processor);
      TServer server = new TThreadedSelectorServer(serverArgs);
      server.serve();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    startAsyncServer();
  }

}