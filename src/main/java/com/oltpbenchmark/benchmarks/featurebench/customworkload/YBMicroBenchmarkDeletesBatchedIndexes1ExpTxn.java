package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

public class YBMicroBenchmarkDeletesBatchedIndexes1ExpTxn extends YBMicroBenchmark {
  public final static Logger LOG =
      Logger.getLogger(com.oltpbenchmark.benchmarks.featurebench.customworkload
                           .YBMicroBenchmarkDeletesBatchedIndexes1ExpTxn.class);
  private static final int NUM_ROWS = 1100;
  private String inClause;

  public YBMicroBenchmarkDeletesBatchedIndexes1ExpTxn(
      HierarchicalConfiguration<ImmutableNode> config) {
    super(config);
    this.loadOnceImplemented = true;

    inClause = "(";
    for (int i = 101; i <= NUM_ROWS; i++) {
      inClause += String.format("%d", i);
      if (i < NUM_ROWS) {
        inClause += ",";
      }
    }
    inClause += ")";
  }

  public void loadOnce(Connection conn) throws SQLException {
    String insertStmt = String.format("call insert_demo(%d);", NUM_ROWS);
    String DeleteStmt = String.format("delete from demo_indexes_1");
    PreparedStatement delete_stmt = conn.prepareStatement(DeleteStmt);
    delete_stmt.execute();
    delete_stmt.close();
    PreparedStatement insert_stmt = conn.prepareStatement(insertStmt);
    insert_stmt.execute();
    insert_stmt.close();
  }

  public void executeOnce(Connection conn) throws SQLException {
    String batchedDeleteStatement =
        String.format("begin; delete from demo_indexes_1 where id in %s; commit;", inClause);
    Statement stmtObj = conn.createStatement();
    stmtObj.execute(batchedDeleteStatement);
    stmtObj.close();
  }
}
