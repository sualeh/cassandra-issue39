package us.fatehi.cassandra_jdbc;

import static org.junit.jupiter.api.Assertions.fail;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@TestInstance(Lifecycle.PER_CLASS)
@Testcontainers
public class CassandraTest {

  private final DockerImageName imageName = DockerImageName.parse("cassandra");

  @Container
  private final CassandraContainer<?> dbContainer =
      new CassandraContainer<>(imageName.withTag("5.0"))
          .withInitScript("create-cassandra-database.cql");

  @Test
  public void testAssertionError() throws Exception {

    if (!dbContainer.isRunning()) {
      fail("Testcontainer for database is not available");
    }

    final InetSocketAddress contactPoint = dbContainer.getContactPoint();
    final String host = contactPoint.getHostName();
    final int port = contactPoint.getPort();
    final String keyspace = "books";
    final String localDatacenter = dbContainer.getLocalDatacenter();
    final String connectionUrl = String.format("jdbc:cassandra://%s:%d/%s?localdatacenter=%s", host,
        port, keyspace, localDatacenter);
    System.out.printf("connection url: %s%n", connectionUrl);

    final DataSource dataSource =
        createDataSource(connectionUrl, dbContainer.getUsername(), dbContainer.getPassword());
    final Connection connection = dataSource.getConnection();
    final DatabaseMetaData databaseMetaData = connection.getMetaData();
    final ResultSet resultSet = databaseMetaData.getSchemas();
    printResultsMetadata(resultSet.getMetaData());
  }

  protected DataSource createDataSource(final String connectionUrl, final String user,
      final String password) {

    final BasicDataSource ds = new BasicDataSource();
    ds.setUrl(connectionUrl);
    ds.setUsername(user);
    ds.setPassword(password);

    return ds;
  }

  protected void printResultsMetadata(final ResultSetMetaData resultsMetaData) throws SQLException {

    final int columnCount = resultsMetaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      final int columnIndex = i;

      final String catalogName = resultsMetaData.getCatalogName(columnIndex);
      final String schemaName = resultsMetaData.getSchemaName(columnIndex);
      final String tableName = resultsMetaData.getTableName(columnIndex);

      final String columnName = resultsMetaData.getColumnName(columnIndex);
      resultsMetaData.getColumnLabel(columnIndex);

      System.out.printf("Column: catalog='%s', schema='%s', table='%s', column='%s'%n", catalogName,
          schemaName, tableName, columnName);

      resultsMetaData.getColumnDisplaySize(columnIndex);
      resultsMetaData.isAutoIncrement(columnIndex);
      resultsMetaData.isCaseSensitive(columnIndex);
      resultsMetaData.isCurrency(columnIndex);
      resultsMetaData.isDefinitelyWritable(columnIndex);
      resultsMetaData.isReadOnly(columnIndex);
      final boolean searchable = resultsMetaData.isSearchable(columnIndex);
      resultsMetaData.isSigned(columnIndex);
      resultsMetaData.isWritable(columnIndex);
      System.out.printf("Column information: searchable='%b'%n", searchable);
    }

  }

}
