package thrift_server;

import org.apache.thrift.TException;
import app.CommandLineRunner;

public class ThriftServiceHandler implements ThriftService.Iface {

  @Override
  public String test(String n1, String n2) throws TException {
    return CommandLineRunner.test(n1, n2);
  }

}