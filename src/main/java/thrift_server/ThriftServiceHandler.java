package thrift_server;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.thrift.TException;

import app.ThriftRunner;
import app.ThriftRunner.InvalidUUIDException;

/**
 * Handels request from thrift by routing them through the {@link ThriftRunner}
 */
public class ThriftServiceHandler implements ThriftService.Iface {

  /**
   * Creates a {@link QueryParser} instance connected to a database defined by the
   * parameters
   * 
   * @param dbUrl    The url to access the database (contains the name of the
   *                 schema that will be accessed)
   * @param user     the username of the user who has permission to access the
   *                 database
   * @param password The password of the supplied user
   * @param name     The name the schema will be called when sending queries to
   *                 calcite (Not for specifying the schema you want to pull from
   *                 the database, that should be specified in the dbUrl)
   * @returns A string which represents a UUID which is needed to identify the
   *          {@link QueryParser} created (for running a query)
   * @throws TException If Thrift had an error or if an error occured while
   *                    accessing the database. If the error occurs when accessing
   *                    the database it should be in the form of a
   *                    {@link JavaException} so it can be identified by its value
   */
  @Override
  public String createInstance(String dbUrl, String user, String password, String name) throws TException {
    try {
      return ThriftRunner.createInstance(dbUrl, user, password, name);
    } catch (Exception e) {
      throw new JavaException("SQLException", e.getMessage());
    }
  }

  /**
   * Runs a query on a {@link QueryParser} referenced by a given id.
   * 
   * NOTE: I want to change the name of this since it doesn't actually
   * <em>run</em> the query, but that would require redoing the thrift file
   * generation so I'm going to wait for our meeting wednesday so I know what
   * other changes I should make
   * 
   * @param id    The id (a UUID in the form of a string) used to find the already
   *              created (via {@code createInstance}) {@link QueryParser}
   * @param query The query to be run on the {@link QueryParser}
   * @returns The structure for running the query as a string
   * @throws InvalidUUIDException When the UUID in id does not correspond with a
   *                              query parser
   * @throws ValidationException  When the query could not be validated as Sql
   * @throws TException           when an error occurs. (usually this type when an
   *                              error with thrift occurs) but can also be for
   *                              other reasons. Look at the error to learn more
   */
  @Override
  public String runQuery(String id, String query) throws TException {
    try {
      return ThriftRunner.getQueryExecutionPlan(id, query);
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