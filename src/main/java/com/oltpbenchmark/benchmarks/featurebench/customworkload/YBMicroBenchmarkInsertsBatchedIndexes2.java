package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

public class YBMicroBenchmarkInsertsBatchedIndexes2 extends YBMicroBenchmark {
  public final static Logger LOG =
      Logger.getLogger(com.oltpbenchmark.benchmarks.featurebench.customworkload
                           .YBMicroBenchmarkInsertsBatchedIndexes2.class);

  public YBMicroBenchmarkInsertsBatchedIndexes2(
      HierarchicalConfiguration<ImmutableNode> config) {
    super(config);
    this.loadOnceImplemented = true;
    this.executeOnceImplemented = true;
  }

  public void loadOnce(Connection conn) throws SQLException {
    String insertStmt = "call insert_demo(100);";
    String DeleteStmt = String.format("delete from demo_indexes_2");
    PreparedStatement delete_stmt = conn.prepareStatement(DeleteStmt);
    delete_stmt.execute();
    delete_stmt.close();
    PreparedStatement insert_stmt = conn.prepareStatement(insertStmt);
    insert_stmt.execute();
    insert_stmt.close();
  }

  public void executeOnce(Connection conn) throws SQLException {
    String values = "";
    for (int i = 101; i <= 1100; i++) {
      values += "(";
      for (int col = 1; col <= 11; col++) {
        values += String.format("%d", i);
        if (col < 11) {
          values += ",";
        }
      }
      values += ")";
      if (i < 1100) {
        values += ",";
      }
    }

    String insertStmt1 = String.format("insert into demo_indexes_2 values %s;", values);
    Statement stmtObj = conn.createStatement();
    stmtObj.execute(insertStmt1);
    stmtObj.close();
  }
}
