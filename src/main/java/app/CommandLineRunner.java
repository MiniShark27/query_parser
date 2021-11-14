package app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

/**
 * Used for running the {@link QueryParser} from the command line on
 * {@code .sql} files
 */
public class CommandLineRunner {

  /**
   * Runs the parser via the command line. Can create the
   * {@code config.properties} file via prompts.
   * 
   * @param args Not used
   * @throws IOException  If the {@code config.properties} file didn't exist, and
   *                      was unable to be created
   * @throws SQLException If a database access error occurs
   */
  public static void main(String... args) throws IOException, SQLException {
    Scanner scanner = new Scanner(System.in);

    Properties connectionProperties;
    try {
      // If config.properties exists
      connectionProperties = GetConnection.readPropertiesFile();
    } catch (Exception e) {
      // If config.properties does not exist (or has an error)
      connectionProperties = GetConnection.createPropertiesFile(scanner);
    }
    QueryParser queryParser = new QueryParser(GetConnection.getSchemaFromProperties(connectionProperties));

    while (true) {
      System.out.println("Enter File with query(s) to execute (type quit to quit):");
      String file = scanner.nextLine();
      if ("quit".equals(file))
        break;
      for (String query : getQueriesFromFile(file)) {
        System.out.println("Query: " + query + "\n");
        try {
          queryParser.getRelNode(query).explain(QueryParser.relWriter);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("");
      }
    }
    scanner.close();
  }

  /**
   * Retreives Sql queries from a given file. Note: will split on {@code ;} so
   * semicolons are required to mark the end of each query in the file
   * 
   * @param fileName The file containing the sql queries. Should be in the same
   *                 directory as where the program was called
   * @return An array of sql queries from the the given file.
   */
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
