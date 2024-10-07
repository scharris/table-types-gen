package tabletypesgen;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.checkerframework.checker.nullness.qual.Nullable;

import static tabletypesgen.util.Args.pluckStringOption;

public class Queryer
{
  final static String USAGE_MSG = """
      Expected arguments: [options] <jdbc-props-file>
        jdbc-props-file: JDBC properties file, with properties jdbc.driverClassName, jdbc.url, jdbc.username, jdbc.password.
        [options]:
          --query-file: File containing the SQL query to be run. Exactly one of --query and --query-file should be specified.
          --query: Text of SQL query to be run. Exactly one of --query and --query-file should be specified.
          --output-file: File to which results will be written. Optional, if absent then output will be written to standard output.
      """;

  public static void main(String[] args)
  {
    if ( args.length == 1 && (args[0].equals("-h") || args[0].equals("--help")) )
    {
      System.out.println(USAGE_MSG);
      return;
    }

    List<String> remArgs = new ArrayList<>(Arrays.asList(args));

    @Nullable Path sqlFile = pluckStringOption(remArgs, "--query-file").map(Paths::get).orElse(null);
    @Nullable String sqlText = pluckStringOption(remArgs, "--query").orElse(null);
    @Nullable Path outputFile = pluckStringOption(remArgs, "--output-file").map(Paths::get).orElse(null);

    if ( remArgs.size() != 1 )
    {
      System.err.println(USAGE_MSG);
      System.exit(1);
    }

    Path jdbcPropsFile = Paths.get(remArgs.get(0));

    if ( !Files.isRegularFile(jdbcPropsFile) )
      throw new RuntimeException("Connection properties file was not found: " + jdbcPropsFile);
    if ( sqlFile == null && sqlText == null || sqlFile != null && sqlText != null )
      throw new RuntimeException("Exactly one of --query and --query-file must be specified.");
    if ( sqlFile != null && !Files.isRegularFile(sqlFile) )
      throw new RuntimeException("SQL file was not found: " + sqlFile);

    try (Connection conn = connect(jdbcPropsFile))
    {
      var queryer = new Queryer();

      String sql = sqlText != null ? sqlText : Files.readString(nn(sqlFile, "query file"));

      queryer.runQuery(sql, conn, outputFile);

      System.exit(0);
    }
    catch(Throwable t)
    {
      System.err.println(t.getMessage());
      System.exit(1);
    }
  }

  private void runQuery
    (
      String sql,
      Connection conn,
      @Nullable Path outPath
    )
    throws IOException, SQLException
  {
    try (var bw = outPath != null ? Files.newBufferedWriter(outPath) : new BufferedWriter(new OutputStreamWriter(System.out));
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql))
    {
      int colCount = rs.getMetaData().getColumnCount();

      while (rs.next())
      {
        for(int cnum = 1; cnum <= colCount; ++cnum)
        {
          if (cnum > 1) bw.append('\t');
          String resStr = rs.getString(cnum);
          String outStr = resStr != null ? resStr : "";
          bw.append(outStr);
        }
      }
    }
    catch(Exception e) { throw new RuntimeException(e); }
  }

  private static Connection connect(Path propsFile)
  {
    try
    {
      Properties props = new Properties();
      props.load(Files.newInputStream(propsFile));

      if (!props.containsKey("jdbc.driverClassName") ||
          !props.containsKey("jdbc.url") ||
          !props.containsKey("jdbc.username") ||
          !props.containsKey("jdbc.password"))
        throw new RuntimeException(
          "Expected connection properties " +
          "{ jdbc.driverClassName, jdbc.url, jdbc.username, jdbc.password } " +
          "in connection properties file."
        );

      Class.forName(nn(props.getProperty("jdbc.driverClassName"), "jdbc.DriverClassName property"));

      return DriverManager.getConnection(
        nn(props.getProperty("jdbc.url"), "jdbc.url property"),
        nn(props.getProperty("jdbc.username"), "jdbc.username property"),
        nn(props.getProperty("jdbc.password"), "jdbc.password property")
      );
    }
    catch(SQLException | ClassNotFoundException | IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  private static <T> T nn(@Nullable T t, String what)
  {
    if (t == null)
      throw new RuntimeException(what + " is required.");
    return t;
  }
}
