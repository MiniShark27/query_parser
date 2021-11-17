package test_thrift_server;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public class ThriftServiceHandler implements ThriftService.AsyncIface, ThriftService.Iface {
  Integer x = 0;

  @Override
  public String test(String n1, String n2) throws TException {
    System.out.println("Start " + n1);
    int y = ++x;
    // try {
    // Thread.sleep((long) Math.floor(1000 * Math.random()));
    // } catch (Exception e) {
    // System.out.println("ERROR");
    // }
    System.out.println("End " + n1);
    return x.toString() + ";" + y;
  }

  @Override
  public void test(String n1, String n2, AsyncMethodCallback<String> resultHandler) throws TException {
    System.out.println("Start " + n1);
    int y = ++x;
    // try {
    // // Thread.sleep((long) Math.floor(4000 * Math.random()));
    // } catch (Exception e) {
    // System.out.println("ERROR");
    // }
    System.out.println("End " + n1);
    resultHandler.onComplete(x.toString() + ";" + y);
  }

}