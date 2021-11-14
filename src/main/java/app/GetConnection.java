package app;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Gets connection information from a {@code config.properties} file (or makes
 * one) and retreives a {@link SchemaPlus} object referenceing the schema
 * defined in the properties file for the {@link QueryParser} to read and parse
 * the queries on
 */
public class GetConnection {
  /**
   * Gets an {@link SchemaPlus} object from a {@link Properties} object that
   * stores the connection properties.
   * 
   * @param props Specifies connection properties, required fields are:
   *              {@code db.url} for the url to connect to the database,
   *              {@code db.user} for the user to connect to the database as,
   *              {@code db.password} the password for the user connecting,
   *              {@code db.newName} the name of the schema in the calcite db
   * @return Schema object (to be used in {@link QueryParser} likely)
   * @throws SQLException if a database access error occurs
   */
  public static SchemaPlus getSchemaFromProperties(Properties props) throws SQLException {
    return getSchema(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"),
        props.getProperty("db.newName"));
  }

  /**
   * Generates {@link SchemaPlus} object from connection information
   * 
   * @param dbUrl    The url to where the database is hosted
   * @param user     The user connecting to the database
   * @param password The password for the user connecting to the database
   * @param newName  The name of the schema in the calcite database that
   *                 references the newly connected schema
   * @return A {@link SchemaPlus} object referenceing the calcite database with
   *         the foreign database's schema added
   * @throws SQLException if a database access error occurs
   */
  private static SchemaPlus getSchema(String dbUrl, String user, String password, String newName) throws SQLException {
    System.out.println("Connecting to database at: " + dbUrl);
    System.out.println("Username: " + user);
    System.out.println("Password: " + password);
    System.out.println("Will be stored in database: " + newName);

    DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    final DataSource ds = JdbcSchema.dataSource(dbUrl, "org.postgresql.Driver", user, password);
    rootSchema.add(newName, JdbcSchema.create(rootSchema, newName, ds, null, null));

    return rootSchema;
  }

  /**
   * Reads the {@code config.properties} file to get a {@code Properties} object
   * 
   * @return the properties object if found and loaded into a properties object
   * @throws IOException If the file is not found or is not able to be parsed
   */
  public static Properties readPropertiesFile() throws IOException {
    Properties prop = new Properties();
    try (InputStream input = new FileInputStream("config.properties")) {
      prop.load(input);
    } catch (IOException ex) {
      System.out.println("Properties file not found or was not able to be parsed");
      throw ex;
    }
    return prop;
  }

  /**
   * Creates a properties file referencing a local postgres database, and returns
   * the associated {@link Properties} object
   * 
   * @param scanner Used to ask the user for information to be used in creating
   *                the properties file/object
   * @return Properties object which mirrors newly created properties file
   * @throws IOException If the file was not able to be created or written to
   */
  public static Properties createPropertiesFile(Scanner scanner) throws IOException {
    System.out.println("Making Properties File");
    System.out.println("Pulling from local postgres database (hosted at 127.0.0.1 i.e. localhost)");

    System.out.println("Please input the name of the maintenence database:");
    String maintenenceDb = scanner.nextLine();
    String newName = maintenenceDb;

    System.out.println("Please input the port where the database is:");
    int port = scanner.nextInt();
    // Clear Scanner buffer
    scanner.nextLine();
    String dbUrl = String.format("jdbc:postgresql://localhost:%d/%s", port, maintenenceDb);

    System.out.println("Please input the username for the database:");
    String username = scanner.nextLine();

    System.out.println("Please input the password for the database:");
    String password = scanner.nextLine();

    OutputStream output = new FileOutputStream("config.properties");
    Properties prop = new Properties();

    prop.setProperty("db.url", dbUrl);
    prop.setProperty("db.user", username);
    prop.setProperty("db.password", password);
    prop.setProperty("db.newName", newName);

    prop.store(output, null);
    System.out.println(prop);
    return prop;
  }
}
