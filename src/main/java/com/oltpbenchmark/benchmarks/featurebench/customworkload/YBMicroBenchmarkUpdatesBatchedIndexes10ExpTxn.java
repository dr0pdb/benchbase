package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

public class YBMicroBenchmarkUpdatesBatchedIndexes10ExpTxn extends YBMicroBenchmark {
  public final static Logger LOG =
      Logger.getLogger(com.oltpbenchmark.benchmarks.featurebench.customworkload
                           .YBMicroBenchmarkUpdatesBatchedIndexes10ExpTxn.class);
  private static final int NUM_ROWS = 1100;

  public YBMicroBenchmarkUpdatesBatchedIndexes10ExpTxn(
      HierarchicalConfiguration<ImmutableNode> config) {
    super(config);
    this.loadOnceImplemented = true;
  }

  public void loadOnce(Connection conn) throws SQLException {
    String insertStmt = String.format("call insert_demo(%d);", NUM_ROWS);
    String DeleteStmt = String.format("delete from demo_indexes_10");
    PreparedStatement delete_stmt = conn.prepareStatement(DeleteStmt);
    delete_stmt.execute();
    delete_stmt.close();
    PreparedStatement insert_stmt = conn.prepareStatement(insertStmt);
    insert_stmt.execute();
    insert_stmt.close();
  }

  public void executeOnce(Connection conn) throws SQLException {
    String inClause = "(";
    for (int i = 101; i <= NUM_ROWS; i++) {
      inClause += String.format("%d", i);
      if (i < NUM_ROWS) {
        inClause += ",";
      }
    }
    inClause += ")";

    // Update last 900 rows.
    String batchedUpdateStatement = String.format(
        "begin; update demo_indexes_10 set col1 = col1 + 10000, col2 = col2 + 10000, col3 = col3 + 10000, col4 = col4 + 10000, col5 = col5 + 10000, col6 = col6 + 10000, col7 = col7 + 10000, col8 = col8 + 10000, col9 = col9 + 10000, col10 = col10 + 10000 where id in %s; commit;",
        inClause);
    Statement stmtObj = conn.createStatement();
    stmtObj.execute(batchedUpdateStatement);
    stmtObj.close();
  }
}
