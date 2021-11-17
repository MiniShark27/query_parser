package app;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class ThriftRunner {
  private static Map<UUID, QueryParser> map = Collections.synchronizedMap(new HashMap<UUID, QueryParser>());

  private static synchronized UUID getUUID() {
    return UUID.randomUUID();
  }

  public static String createInstance(String dbUrl, String user, String password, String name) throws SQLException {
    UUID id = getUUID();
    map.put(id, new QueryParser(dbUrl, user, password, name));
    return id.toString();
  }

  public static String runQuery(String id, String query)
      throws InvalidUUIDException, SqlParseException, ValidationException, RelConversionException {
    UUID uuid = UUID.fromString(id);
    QueryParser queryParser = map.get(uuid);
    if (queryParser == null)
      throw new InvalidUUIDException(id + " does not correspond to a valid UUID");
    return QueryParser.relNodeToString(queryParser.getRelNode(query));
  }

  public static class InvalidUUIDException extends Exception {
    public InvalidUUIDException(String str) {
      super(str);
    }
  }
}
