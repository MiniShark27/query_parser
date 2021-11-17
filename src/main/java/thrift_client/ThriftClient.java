package thrift_client;

import java.io.IOException;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import app.CommandLineRunner;
import app.GetConnection;
import thrift_server.JavaException;
import thrift_server.ThriftService;
//To convert to async use this website https://github.com/helloworlde/thrift-java-sample/tree/main/async-server

public class ThriftClient {

  /**
   * Sample Client in java, will need one in cpp but I don't have a cpp project
   * setup and had issues with lines stating {@code #include <thrift>} not finding
   * thrift (likely because I didn't install or put it in the right place)
   * 
   * @throws IOException If the proporties file doesn't exist. It can be created
   *                     by running the main method in {@link CommandLineRunner}
   *                     or own your own if you know how
   */
  public static void main(String[] args) throws IOException {
    // Get database connection details from properties file
    Properties props = GetConnection.readPropertiesFile();
    System.out.println(props);
    try {
      TTransport transport;
      transport = new TSocket("localhost", 9090);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      ThriftService.Client client = new ThriftService.Client(protocol);

      // Here is where we actually call the Query Parser
      String id = client.createInstance(props.getProperty("db.url"), props.getProperty("db.user"),
          props.getProperty("db.password"), props.getProperty("db.name"));
      System.out.println(id);
      String res = client.runQuery(id, "select * from postgres.hi");
      System.out.println(res);
      res = client.runQuery(id, "select * from postgres.hi natural join postgres.hi2");
      System.out.println(res);
      // End of calls to Query Parser

      transport.close();
    } catch (TTransportException e) {
      e.printStackTrace();
    } catch (JavaException x) {
      System.out.println("Java Exception");
      x.printStackTrace();
    } catch (TException x) {
      x.printStackTrace();
    }
  }

}
