package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

public class YBMicroBenchmarkInsertsSequentialIndexes10ExpTxn extends YBMicroBenchmark {
  public final static Logger LOG =
      Logger.getLogger(com.oltpbenchmark.benchmarks.featurebench.customworkload
                           .YBMicroBenchmarkInsertsSequentialIndexes10ExpTxn.class);

  public YBMicroBenchmarkInsertsSequentialIndexes10ExpTxn(
      HierarchicalConfiguration<ImmutableNode> config) {
    super(config);
    this.loadOnceImplemented = true;
    this.executeOnceImplemented = true;
  }

  public void loadOnce(Connection conn) throws SQLException {
    String insertStmt = "call insert_demo(100);";
    String DeleteStmt = String.format("delete from demo_indexes_10");
    PreparedStatement delete_stmt = conn.prepareStatement(DeleteStmt);
    delete_stmt.execute();
    delete_stmt.close();
    PreparedStatement insert_stmt = conn.prepareStatement(insertStmt);
    insert_stmt.execute();
    insert_stmt.close();
  }

  public void executeOnce(Connection conn) throws SQLException {
    Statement stmtObj = conn.createStatement();
    for (int id = 101; id <= 1100; id++) {
      String query = String.format(
          "begin; insert into demo_indexes_10 values (%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d); commit;",
          id, id, id, id, id, id, id, id, id, id, id);
      stmtObj.execute(query);
    }
    stmtObj.close();
  }
}
