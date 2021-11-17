package app;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import thrift_server.ThriftServiceHandler;
import thrift_server.ThriftService.AsyncProcessor.runQuery;

/**
 * For methods that bridge the gap between the {@link QueryParser} and the
 * {@link ThriftServiceHandler}
 */
public class ThriftRunner {
  // Stores the created query parsers
  private static Map<UUID, QueryParser> map = Collections.synchronizedMap(new HashMap<UUID, QueryParser>());

  /**
   * Generates a unique UUID. Done in its own method to mark as
   * {@code synchronized} to allow for asynchronous use (to make sure there can't
   * be duplicate UUIDs).
   * 
   * @return A unique UUID
   */
  private static synchronized UUID getUUID() {
    return UUID.randomUUID();
  }

  /**
   * Creates a {@link QueryParser} connected to a database using the given
   * credentials (in the parameters) and creates a unique id that can be used in
   * {@link runQuery} to run queries on the {@link QueryParser}
   * 
   * @param dbUrl    The url referencing the database that will be connected to by
   *                 the new query parser. Note the schema accessed should be
   *                 referenced here (as the maintenece schema)
   * @param user     The user who has access to the database at {@code dbUrl}
   * @param password The password for the user
   * @param name     The new name of the schema in the calcite database. Not the
   *                 name of the schema being pulled from the database at
   *                 {@code dbUrl}
   * @return If Successful an id string (in the form of a UUID) that will be used
   *         in {@link runQuery} to run a query on the new {@link QueryParser}
   * @throws SQLException If the {@link QueryParser} was not able to be made.
   *                      Often due to not being able to connect with the database
   *                      with the given credentail, or there not being a database
   *                      at the given {@code dbUrl}
   */
  public static String createInstance(String dbUrl, String user, String password, String name) throws SQLException {
    UUID id = getUUID();
    map.put(id, new QueryParser(dbUrl, user, password, name));
    return id.toString();
  }

  /**
   * Generates an execution plan for the {@code query} using the
   * {@link QueryParser} referenced by {@code id} and
   * 
   * @param id    the id corresponding to the {@link QueryParser} that will
   *              generate the execution plan. This will be in the form of a UUID
   *              as a string
   * @param query the query the plan will be made for
   * @return A string representation of the query plan
   * @throws InvalidUUIDException   If the id given does not correspond to a
   *                                {@link QueryParser} already created
   * @throws SqlParseException      If the query was not able to be parsed
   * @throws ValidationException    If the query could not be validated
   * @throws RelConversionException If the query could not be converted to a
   *                                {@link RelNode}
   */
  public static String getQueryExecutionPlan(String id, String query)
      throws InvalidUUIDException, SqlParseException, ValidationException, RelConversionException {
    UUID uuid = UUID.fromString(id);
    QueryParser queryParser = map.get(uuid);
    if (queryParser == null)
      throw new InvalidUUIDException(id + " does not correspond to a valid UUID");
    return QueryParser.relNodeToString(queryParser.getRelNode(query));
  }

  /**
   * Exception for if a UUID given to {@link runQuery} is invalid (i.e does not
   * correspond to a valid {@link QueryParser})
   */
  public static class InvalidUUIDException extends Exception {
    public InvalidUUIDException(String str) {
      super(str);
    }
  }
}
