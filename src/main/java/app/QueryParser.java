package app;

import java.io.PrintWriter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

/**
 * Parses queries into a lower level than sql to be read by a database.
 */
public class QueryParser {

  SchemaPlus schema;
  FrameworkConfig config;
  static RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      false);

  /**
   * Constructs the {@link QueryParser} object referencing the given schema
   * 
   * @param schema The Schema this {@link QueryParser} will reference
   */
  public QueryParser(SchemaPlus schema) {
    this.schema = schema;
    SqlParser.Config insensitiveParser = SqlParser.config().withCaseSensitive(false);
    this.config = Frameworks.newConfigBuilder().parserConfig(insensitiveParser).defaultSchema(this.schema).build();
  }

  /**
   * Gets a {@link RelNode} for the given query
   * 
   * @param query The query that is being converted to a {@link RelNode}
   * @return The {@link RelNode} formed for the given query
   * @throws SqlParseException      If the query was not able to be parsed
   * @throws ValidationException    If the query could not be validated
   * @throws RelConversionException If the query could not be converted to a
   *                                {@link RelNode}
   */
  public RelNode getRelNode(String query) throws SqlParseException, ValidationException, RelConversionException {
    Planner planner = Frameworks.getPlanner(config);
    SqlNode sqlNode = planner.parse(query);
    SqlNode sqlNodeValidated = planner.validate(sqlNode);
    RelRoot relRoot = planner.rel(sqlNodeValidated);
    return relRoot.project();
  }
}