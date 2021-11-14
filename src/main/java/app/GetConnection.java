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

public class GetConnection {

  public static SchemaPlus getSchemaFromProperties(Properties prop) throws SQLException {
    return getSchema(prop.getProperty("db.url"), prop.getProperty("db.user"), prop.getProperty("db.password"),
          prop.getProperty("db.calciteDb"));
  }

  private static SchemaPlus getSchema(String dbUrl, String username, String password, String calciteDb)
      throws SQLException {
    System.out.println("Connecting to database at: " +dbUrl);
    System.out.println("Username: "+username);
    System.out.println("Password: "+password);
    System.out.println("Will be stored in database: " +calciteDb);
    DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    final DataSource ds = JdbcSchema.dataSource(dbUrl, "org.postgresql.Driver", username, password);
    rootSchema.add(calciteDb, JdbcSchema.create(rootSchema, calciteDb, ds, null, null));
    return rootSchema;
  }

  public static Properties readPropertiesFile() throws IOException{
    Properties prop = new Properties();
    try (InputStream input = new FileInputStream("config.properties")) {
      prop.load(input);
    } catch (IOException ex) {
      System.out.println("Properties File Not Found");
      throw ex;
    }
    return prop;
  }

  public static Properties makePostgresSchemaProperties(Scanner scanner) throws IOException {
    System.out.println("Making Properties File");
    System.out.println("Pulling from local postgres database (hosted at 127.0.0.1 i.e. localhost)");

    System.out.println("Please input the name of the maintenence database:");
    String maintenenceDb = scanner.nextLine();
    String calciteDb = maintenenceDb;

    System.out.println("Please input the port where the database is:");
    int port = scanner.nextInt();
    //Clear Scanner buffer
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
    prop.setProperty("db.calciteDb", calciteDb);

    prop.store(output, null);
    System.out.println(prop);
    return prop;
  }
}
