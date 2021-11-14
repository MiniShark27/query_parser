package app;

import java.io.PrintWriter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
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

  public static void printQueryPlan(String query, FrameworkConfig config) {
    try {
      Planner planner = Frameworks.getPlanner(config);
      System.out.println("\nQuery read from file:");
      System.out.println(query);
      SqlNode sqlNode = planner.parse(query);
      System.out.println("\nParsed Query (to sql):");
      System.out.println(sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT));
      SqlNode sqlNodeValidated = planner.validate(sqlNode);
      RelRoot relRoot = planner.rel(sqlNodeValidated);
      RelNode relNode = relRoot.project();
      System.out.println("\nParsed Query:");
      final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.EXPPLAN_ATTRIBUTES,
          false);
      relNode.explain(relWriter);
    } catch (SqlParseException e) {
      System.out.println("ERROR Failed to parse");
      System.out.println(e);
    } catch (ValidationException e) {
      System.out.println("ERROR invalid command");
      System.out.println(e);
    } catch (RelConversionException e) {
      System.out.println("ERROR invalid conversion");
      System.out.println(e);
    }
  }

  public RelNode getRelNode(String query) throws SqlParseException, ValidationException, RelConversionException {
    Planner planner = Frameworks.getPlanner(config);
    // System.out.println("\nQuery read from file:");
    // System.out.println(query);
    SqlNode sqlNode = planner.parse(query);
    // System.out.println("\nParsed Query (to sql):");
    // System.out.println(sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT));
    SqlNode sqlNodeValidated = planner.validate(sqlNode);
    RelRoot relRoot = planner.rel(sqlNodeValidated);
    RelNode relNode = relRoot.project();
    // System.out.println("\nParsed Query:");
    // relNode.explain(relWriter);
    return relNode;
  }
}