/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.featurebench;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.workerhelpers.Query;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.TimeUtil;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 */
public class FeatureBenchWorker extends Worker<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchWorker.class);
    static AtomicBoolean isCleanUpDone = new AtomicBoolean(false);
    public String workloadClass = null;
    public HierarchicalConfiguration<ImmutableNode> config = null;
    public YBMicroBenchmark ybm = null;
    public List<ExecuteRule> executeRules = null;
    public String workloadName = "";

    public FeatureBenchWorker(FeatureBenchBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    protected void initialize() {


        if (this.getWorkloadConfiguration().getXmlConfig().containsKey("collect_pg_stat_statements") &&
            this.getWorkloadConfiguration().getXmlConfig().getBoolean("collect_pg_stat_statements")) {
            LOG.info("Resetting pg_stat_statements");
            try {
                Statement stmt = conn.createStatement();
                stmt.executeQuery("SELECT pg_stat_statements_reset();");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        String outputDirectory = "results";
        FileUtil.makeDirIfNotExists(outputDirectory);
        String explainDir = "ResultsForExplain";
        FileUtil.makeDirIfNotExists(outputDirectory + "/" + explainDir);
        String fileForExplain = explainDir + "/" + workloadName + "_" + TimeUtil.getCurrentTimeString() + ".json";
        PrintStream ps;
        String explainSelect = "explain (analyze,verbose,costs,buffers) ";
        String explainUpdate = "explain (analyze) ";

        try {
            ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileForExplain));
        } catch (FileNotFoundException exc) {
            throw new RuntimeException(exc);
        }

        List<PreparedStatement> explainDDLs = new ArrayList<>();

        for (ExecuteRule er : executeRules) {
            for (Query query : er.getQueries()) {
                if (query.isSelectQuery() || query.isUpdateQuery()) {
                    String querystmt = query.getQuery();
                    PreparedStatement stmt = null;
                    try {

                        stmt = conn.prepareStatement((query.isSelectQuery() ? explainSelect : explainUpdate) + querystmt);
                        List<UtilToMethod> baseUtils = query.getBaseUtils();
                        for (int j = 0; j < baseUtils.size(); j++) {
                            try {
                                stmt.setObject(j + 1, baseUtils.get(j).get());
                            } catch (SQLException | InvocationTargetException | IllegalAccessException |
                                     ClassNotFoundException | NoSuchMethodException | InstantiationException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        explainDDLs.add(stmt);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }
        try {
            if (explainDDLs.size() > 0)
                writeExplain(ps, explainDDLs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void writeExplain(PrintStream os, List<PreparedStatement> explainDDLs) throws SQLException {
        LOG.info("Running explain for select/update queries before execute phase");
        Map<String, JSONObject> summaryMap = new TreeMap<>();
        int count = 0;
        for (PreparedStatement ddl : explainDDLs) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ddl", ddl);
            count++;
            int countResultSetGen = 0;
            while (countResultSetGen < 3) {
                ddl.executeQuery();
                countResultSetGen++;
            }
            long explainStart = System.currentTimeMillis();
            ResultSet rs = ddl.executeQuery();
            StringBuilder data = new StringBuilder();
            while (rs.next()) {
                data.append(rs.getString(1));
                data.append(" ");
            }
            long explainEnd = System.currentTimeMillis();
            jsonObject.put("ResultSet", data.toString());
            jsonObject.put("Time(ms) ", explainEnd - explainStart);
            summaryMap.put("ExplainDDL" + count, jsonObject);
        }
        os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }


    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws
        UserAbortException, SQLException {


        try {
            ybm = (YBMicroBenchmark) Class.forName(workloadClass)
                .getDeclaredConstructor(HierarchicalConfiguration.class)
                .newInstance(config);

            if (config.containsKey("execute") && config.getBoolean("execute")) {
                ybm.execute(conn);
                return TransactionStatus.SUCCESS;
            } else if (executeRules == null || executeRules.size() == 0) {
                if (this.configuration.getWorkloadState().getGlobalState() == State.MEASURE) {
                    ybm.executeOnce(conn);
                }
                return TransactionStatus.SUCCESS;
            }

            int executeRuleIndex = txnType.getId() - 1;
            ExecuteRule executeRule = executeRules.get(executeRuleIndex);
            for (Query query : executeRule.getQueries()) {
                String querystmt = query.getQuery();
                PreparedStatement stmt = conn.prepareStatement(querystmt);
                List<UtilToMethod> baseUtils = query.getBaseUtils();
                int count = query.getCount();
                for (int i = 0; i < count; i++) {
                    for (int j = 0; j < baseUtils.size(); j++) {
                        stmt.setObject(j + 1, baseUtils.get(j).get());
                    }
                    if (query.isSelectQuery()) {
                        ResultSet rs = stmt.executeQuery();
                        int countSet = 0;
                        while (rs.next()) {
                            countSet++;
                        }
                        if (countSet == 0) {
                            return TransactionStatus.RETRY;
                        }
                    } else {
                        int updatedRows = stmt.executeUpdate();
                        if (updatedRows == 0) {
                            return TransactionStatus.RETRY;
                        }
                    }
                }
            }

        } catch (ClassNotFoundException | InvocationTargetException
                 | InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return TransactionStatus.SUCCESS;
    }


    @Override
    public void tearDown() {
        if (!this.configuration.getNewConnectionPerTxn() && this.configuration.getWorkloadState().getGlobalState() == State.EXIT) {
            if (this.getWorkloadConfiguration().getXmlConfig().containsKey("collect_pg_stat_statements") &&
                this.getWorkloadConfiguration().getXmlConfig().getBoolean("collect_pg_stat_statements")) {
                LOG.info("Collecting pg_stat_statements");
                try {
                    excutePgStatStatements();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!this.configuration.getNewConnectionPerTxn() && this.conn != null && ybm != null) {
            try {
                if (this.configuration.getWorkloadState().getGlobalState() == State.EXIT && !isCleanUpDone.get()) {
                    if (config.containsKey("cleanup")) {
                        LOG.info("\n=================Cleanup Phase taking from Yaml=========\n");
                        List<String> ddls = config.getList(String.class, "cleanup");
                        try {
                            Statement stmtObj = conn.createStatement();
                            for (String ddl : ddls) {
                                stmtObj.execute(ddl);
                            }
                            stmtObj.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                    } else {
                        ybm.cleanUp(conn);
                    }
                    conn.close();
                    isCleanUpDone.set(true);
                }
            } catch (SQLException e) {
                LOG.error("Connection couldn't be closed.", e);
            }
        }
    }

    private void excutePgStatStatements() throws SQLException {
        String pgStatDDL = "select * from pg_stat_statements;";
        String PgStatsDir = "ResultsForPgStats";
        FileUtil.makeDirIfNotExists("results" + "/" + PgStatsDir);
        String fileForPgStats = PgStatsDir + "/" + workloadName + "_" + TimeUtil.getCurrentTimeString() + ".json";
        PrintStream ps;
        try {
            ps = new PrintStream(FileUtil.joinPath("results", fileForPgStats));
        } catch (FileNotFoundException exc) {
            throw new RuntimeException(exc);
        }

        Map<String, JSONObject> summaryMap = new TreeMap<>();
        Statement stmt = this.getBenchmark().makeConnection().createStatement();
        JSONObject outer = new JSONObject();
        int count = 0;
        ResultSet resultSet = stmt.executeQuery(pgStatDDL);
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            JSONObject inner = new JSONObject();
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                inner.put(rsmd.getColumnName(i), columnValue);
            }
            outer.put("Record_" + count, inner);
            count++;
        }
        summaryMap.put("PgStats", outer);
        ps.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }
}