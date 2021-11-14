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

public class QueryParser {
  SchemaPlus schema;
  FrameworkConfig config;
  static RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      false);

  public QueryParser(SchemaPlus schema) {
    this.schema = schema;
    SqlParser.Config insensitiveParser = SqlParser.config().withCaseSensitive(false);
    this.config = Frameworks.newConfigBuilder().parserConfig(insensitiveParser).defaultSchema(this.schema).build();
  }

  public RelNode getRelNode(String query) {
    RelNode toReturn = null;
    try {
      Planner planner = Frameworks.getPlanner(config);
      SqlNode sqlNode = planner.parse(query);
      SqlNode sqlNodeValidated = planner.validate(sqlNode);
      RelRoot relRoot = planner.rel(sqlNodeValidated);
      toReturn = relRoot.project();
    } catch (SqlParseException e) {
      System.out.println("ERROR Failed to parse query");
      System.out.println(e);
    } catch (ValidationException e) {
      System.out.println("ERROR invalid query (failed validation)");
      System.out.println(e);
    } catch (RelConversionException e) {
      System.out.println("ERROR invalid conversion query tree");
      System.out.println(e);
    }
    return toReturn;
  }
}