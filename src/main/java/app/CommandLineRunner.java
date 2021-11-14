package app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.calcite.rel.RelNode;

public class CommandLineRunner {
  public static void main(String... args) throws SQLException, IOException{
    Scanner scanner = new Scanner(System.in);
    Properties connectionProperties;
    try {
      connectionProperties = GetConnection.readPropertiesFile();
    } catch (Exception e) {
      connectionProperties = GetConnection.makePostgresSchemaProperties(scanner);
    }
    QueryParser qp = new QueryParser(GetConnection.getSchemaFromProperties(connectionProperties));
    while (true) {
      System.out.println("Enter File with query(s) to execute (type quit to quit):");
      String file = scanner.nextLine();
      if ("quit".equals(file))
        break;
      for (String query : getQueriesFromFile(file)){
        System.out.println("Query: " + query + "\n");
        RelNode relNode = qp.getRelNode(query);
        relNode.explain(QueryParser.relWriter);
        System.out.println("");
      }
    }
    scanner.close();
  }

  public static String[] getQueriesFromFile(String fileName) {
    String input = "";
    try {
      File f = new File(fileName);
      Scanner reader = new Scanner(f);
      while (reader.hasNextLine()) {
        input += reader.nextLine();
      }
      reader.close();
    } catch (FileNotFoundException e) {
      System.out.println("ERROR File not found");
    }
    return input.split(";");
  }
}
