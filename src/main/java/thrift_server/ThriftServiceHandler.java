package thrift_server;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.thrift.TException;

import app.ThriftRunner;
import app.ThriftRunner.InvalidUUIDException;

public class ThriftServiceHandler implements ThriftService.Iface {

  @Override
  public String createInstance(String dbUrl, String user, String password, String name) throws TException {
    System.out.println("HI");
    try {
      return ThriftRunner.createInstance(dbUrl, user, password, name);
    } catch (Exception e) {
      throw new JavaException("SQLException", e.getMessage());
    }
  }

  @Override
  public String runQuery(String id, String query) throws TException {
    try {
      return ThriftRunner.runQuery(id, query);
    } catch (InvalidUUIDException e) {
      throw new JavaException("InvalidUUIDException", e.getMessage());
    } catch (SqlParseException e) {
      throw new JavaException("SqlParseException", e.getMessage());
    } catch (ValidationException e) {
      throw new JavaException("ValidationException", e.getMessage());
    } catch (RelConversionException e) {
      throw new JavaException("RelConversionException", e.getMessage());
    } catch (RuntimeException e) {
      throw new JavaException("RuntimeException", e.getMessage());
    } catch (Exception e) {
      throw new JavaException("Exception", e.getMessage());
    }
  }
}