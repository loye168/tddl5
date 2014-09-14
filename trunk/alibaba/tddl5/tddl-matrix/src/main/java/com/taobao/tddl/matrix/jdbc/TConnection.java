package com.taobao.tddl.matrix.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.ITransactionPolicy;
import com.taobao.tddl.common.jdbc.ITransactionPolicy.TransactionType;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.model.SqlMetaData;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.common.model.ThreadLocalString;
import com.taobao.tddl.common.model.hint.DirectlyRouteCondition;
import com.taobao.tddl.common.plugin.PreSqlPlugin;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.executor.cursor.IResultSetCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.transaction.AllowReadTransaction;
import com.taobao.tddl.executor.transaction.AutoCommitTransaction;
import com.taobao.tddl.executor.transaction.CobarStyleTransaction;
import com.taobao.tddl.executor.transaction.StrictlTransaction;
import com.taobao.tddl.group.jdbc.ThreadLocalDataSourceIndex;
import com.taobao.tddl.group.utils.GroupHintParser;
import com.taobao.tddl.matrix.jdbc.utils.ExceptionUtils;
import com.taobao.tddl.monitor.unit.RouterUnitsHelper;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.hint.SimpleHintParser;
import com.taobao.tddl.statistics.SQLTracer;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.0.0
 */
public class TConnection implements IConnection {

    private final static Logger    logger               = LoggerFactory.getLogger(TConnection.class);
    public static String           TRACE                = "trace";
    private MatrixExecutor         executor             = null;
    private final TDataSource      ds;
    private ExecutionContext       executionContext     = new ExecutionContext();                                    // 记录上一次的执行上下文
    private final List<TStatement> openedStatements     = Collections.synchronizedList(new ArrayList<TStatement>(2));
    private boolean                isAutoCommit         = true;                                                      // jdbc规范，新连接为true
    private boolean                closed;
    private int                    transactionIsolation = -1;
    private final ExecutorService  executorService;

    private TransactionType        trxConfig            = TransactionType.STRICT;

    private ITransactionPolicy     trxPolicy            = ITransactionPolicy.ALLOW_READ_CROSS_DB;
    /**
     * 管理这个连接下用到的所有物理连接
     */
    private long                   lastInsertId;
    private String                 encoding             = null;
    private String                 sqlMode              = null;
    private List<Long>             generatedKeys;
    private ITransaction           trx;
    private SQLTracer              tracer;

    public TConnection(TDataSource ds){
        this.ds = ds;
        this.executor = ds.getExecutor();
        this.executorService = ds.borrowExecutorService();
    }

    protected boolean isWrite(String sql) {
        SqlAnalysisResult result = OptimizerContext.getContext().getSqlParseManager().parse(sql, true);
        SqlType type = result.getSqlType();
        switch (type) {
            case INSERT:
            case UPDATE:
            case REPLACE:
            case DELETE:
                return true;
            default:
                return false;
        }
    }

    /**
     * 执行sql语句的逻辑
     */
    public ResultSet executeSQL(String sql, Parameters params, TStatement stmt, Map<String, Object> extraCmd,
                                ExecutionContext executionContext) throws SQLException {
        this.trxConfig = this.trxPolicy.getTransactionType(isAutoCommit);
        if (this.trx == null || this.trx.isClosed()) {
            beginTransaction();
        }

        try {
            List<PreSqlPlugin> plugins = this.ds.getPreSqlPluginList();
            if (!GeneralUtil.isEmpty(plugins)) {
                for (PreSqlPlugin plugin : plugins) {
                    sql = plugin.handle(sql);
                }
            }

            int trace = traceIndex(sql);
            if (trace > 0) {
                sql = sql.substring(trace);
                this.tracer = new SQLTracer();
                executionContext.setEnableTrace(true);
            } else {
                executionContext.setEnableTrace(false);
            }

            executionContext.setTracer(this.tracer);

            ExecutorContext.setContext(this.ds.getConfigHolder().getExecutorContext());
            OptimizerContext.setContext(this.ds.getConfigHolder().getOptimizerContext());

            ResultCursor resultCursor;
            ResultSet rs = null;
            extraCmd.putAll(buildExtraCommand(sql));
            // 处理下group hint
            String groupHint = GroupHintParser.extractTDDLGroupHint(sql);
            if (StringUtils.isNotEmpty(groupHint)) {
                sql = GroupHintParser.removeTddlGroupHint(sql);
                executionContext.setGroupHint(GroupHintParser.buildTddlGroupHint(groupHint));
            } else {
                executionContext.setGroupHint(null);
            }
            executionContext.setExecutorService(executorService);
            executionContext.setParams(params);
            executionContext.setSql(sql);
            executionContext.setExtraCmds(extraCmd);
            executionContext.setTransaction(trx);
            executionContext.setTxIsolation(transactionIsolation);
            executionContext.setSqlMode(sqlMode);
            executionContext.setEncoding(encoding);
            executionContext.setConnection(this);
            try {
                resultCursor = executor.execute(sql, executionContext);
            } catch (TddlException e) {
                logger.error("error when executeSQL, sql is: " + sql, e);
                throw new TddlNestableRuntimeException(e);
            }

            if (resultCursor instanceof IResultSetCursor) {
                rs = ((IResultSetCursor) resultCursor).getResultSet();
            } else {
                rs = new TResultSet(resultCursor);
            }

            return rs;
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
        }
    }

    private int traceIndex(String sql) {
        String temp = sql;
        int i = 0;
        for (; i < temp.length(); ++i) {
            switch (temp.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }
            break;
        }

        if (temp.toLowerCase().startsWith(TRACE, i)) {
            return i + TRACE.length();
        } else {
            return -1;
        }
    }

    @Override
    public TPreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TPreparedStatement stmt = new TPreparedStatement(ds, this, sql, context);
        openedStatements.add(stmt);
        return stmt;
    }

    @Override
    public TStatement createStatement() throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TStatement stmt = new TStatement(ds, this, context);
        openedStatements.add(stmt);
        return stmt;
    }

    private ExecutionContext prepareExecutionContext() throws SQLException {
        if (isAutoCommit) {
            if (this.executionContext != null) {
                this.executionContext.cleanTempTables();
            }

            // 即使为autoCommit也需要记录
            // 因为在JDBC规范中，只要在statement.execute执行之前,设置autoCommit=false都是有效的
            this.executionContext = new ExecutionContext();

        } else {
            if (this.executionContext == null) {
                this.executionContext = new ExecutionContext();
            }

            if (this.executionContext.isAutoCommit()) {
                this.executionContext.setAutoCommit(false);
            }
        }

        this.executionContext.setRecorder(this.ds.getRecorder());
        return this.executionContext;
    }

    /*
     * ========================================================================
     * JDBC事务相关的autoCommit设置、commit/rollback、TransactionIsolation等
     * ======================================================================
     */

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit;

        if (this.trx != null) {
            this.trx.commit();
            this.trx = null;
        }

        if (this.executionContext != null) {
            this.executionContext.setAutoCommit(autoCommit);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();

        if (this.trx != null) {
            try {
                // 事务结束,清理事务内容
                this.trx.commit();
            } catch (TddlException e) {
                throw new TddlNestableRuntimeException(e);
            } finally {
                this.trx.close();
            }
        }

        removeTxcContext();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();

        if (this.trx != null) {
            try {
                this.trx.rollback();
            } catch (TddlException e) {
                throw new TddlNestableRuntimeException(e);
            } finally {
                this.trx.close();
            }
        }

        removeTxcContext();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new TDatabaseMetaData(ds);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            List<SQLException> exceptions = new LinkedList<SQLException>();
            try {
                // 关闭statement
                for (TStatement stmt : openedStatements) {
                    try {
                        stmt.close(false);
                    } catch (SQLException e) {
                        exceptions.add(e);
                    }
                }
            } finally {
                openedStatements.clear();
            }

            if (executorService != null) {
                this.ds.releaseExecutorService(executorService);
            }
            if (this.executionContext != null) {
                this.executionContext.cleanTempTables();
            }

            closed = true;
            ExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
        } finally {
            if (this.trx != null) {
                this.trx.close();
            }

            flush_hint();
            RouterUnitsHelper.clearUnitValidThreadLocal();
            ThreadLocalDataSourceIndex.clearIndex();
        }

    }

    private Map<String, Object> buildExtraCommand(String sql) {
        Map<String, Object> extraCmd = new HashMap();
        String andorExtra = "/* ANDOR ";
        String tddlExtra = "/* TDDL ";
        if (sql != null) {
            String commet = TStringUtil.substringAfter(sql, tddlExtra);
            // 去掉注释
            if (TStringUtil.isNotEmpty(commet)) {
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isEmpty(commet) && sql.startsWith(andorExtra)) {
                commet = TStringUtil.substringAfter(sql, andorExtra);
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isNotEmpty(commet)) {
                String[] params = commet.split(",");
                for (String param : params) {
                    String[] keyAndVal = param.split("=");
                    if (keyAndVal.length != 2) {
                        throw new IllegalArgumentException(param + " is wrong , only key = val supported");
                    }
                    String key = keyAndVal[0];
                    String val = keyAndVal[1];
                    extraCmd.put(key, val);
                }
            }
        }
        extraCmd.putAll(this.ds.getConnectionProperties());
        return extraCmd;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        TStatement stmt = createStatement();
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public TStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                            throws SQLException {
        TStatement stmt = (TStatement) createStatement(resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setAutoGeneratedKeys(autoGeneratedKeys);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql, resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnIndexes(columnIndexes);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnNames(columnNames);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                       throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        checkClosed();
        // 先检查下是否有db hint
        DirectlyRouteCondition route = (DirectlyRouteCondition) SimpleHintParser.getRouteContiongFromThreadLocal(ThreadLocalString.DB_SELECTOR);
        String defaultDbIndex = null;
        if (route != null) {
            defaultDbIndex = route.getDbId();
        } else {
            // 针对存储过程，直接下推到default库上执行
            defaultDbIndex = this.ds.getConfigHolder().getOptimizerContext().getRule().getDefaultDbIndex(null);
        }

        IGroupExecutor groupExecutor = this.ds.getConfigHolder()
            .getExecutorContext()
            .getTopologyHandler()
            .get(defaultDbIndex);

        Object groupDataSource = groupExecutor.getRemotingExecutableObject();
        if (groupDataSource instanceof DataSource) {
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            Connection conn = ((DataSource) groupDataSource).getConnection();
            CallableStatement target = null;
            if (resultSetType != Integer.MIN_VALUE && resultSetHoldability != Integer.MIN_VALUE) {
                target = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if (resultSetType != Integer.MIN_VALUE) {
                target = conn.prepareCall(sql, resultSetType, resultSetConcurrency);
            } else {
                target = conn.prepareCall(sql);
            }

            ExecutionContext context = prepareExecutionContext();
            TCallableStatement stmt = new TCallableStatement(ds, this, sql, context, target);
            openedStatements.add(stmt);
            return stmt;
        } else {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (this.transactionIsolation == level) {
            return;
        }

        this.transactionIsolation = level;
        if (executionContext != null) {
            executionContext.setTxIsolation(level);
        }
    }

    @Override
    public String getSqlMode() {
        return sqlMode;
    }

    @Override
    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;

        if (executionContext != null) {
            executionContext.setSqlMode(sqlMode);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    /**
     * 暂时实现为isClosed
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.isClosed();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        /*
         * 如果你看到这里，那么恭喜，哈哈 mysql默认在5.x的jdbc driver里面也没有实现holdability 。
         * 所以默认都是.CLOSE_CURSORS_AT_COMMIT 为了简化起见，我们也就只实现close这种
         */
        throw new UnsupportedOperationException("setHoldability");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public boolean removeStatement(Object arg0) {
        return openedStatements.remove(arg0);
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // do nothing
    }

    /**
     * 保持可读可写
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /*---------------------后面是未实现的方法------------------------------*/

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("setTypeMap");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("nativeSQL");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog");
    }

    public IConnectionHolder getConnectionHolder() {
        return this.trx.getConnectionHolder();
    }

    @Override
    public void kill() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();

        if (this.trx != null) {
            trx.kill();
        }

        try {
            this.close();
        } catch (SQLException e) {
            exceptions.add(e);
        }

        ExceptionUtils.throwSQLException(exceptions, "kill tconnection", Collections.EMPTY_LIST);

    }

    @Override
    public void cancelQuery() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();

        if (this.executionContext != null) {
            this.executionContext.cleanTempTables();
        }

        if (this.trx != null) {
            trx.cancel();
        }
        try {
            // 关闭statement
            for (TStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }
        } finally {
            openedStatements.clear();
        }

        ExceptionUtils.throwSQLException(exceptions, "cancleQuery tconnection", Collections.EMPTY_LIST);

    }

    @Override
    public long getLastInsertId() {
        return this.lastInsertId;
    }

    @Override
    public void setLastInsertId(long id) {
        this.lastInsertId = id;
    }

    @Override
    public List<Long> getGeneratedKeys() {
        return generatedKeys;
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
        generatedKeys = ids;
    }

    @Override
    public void setEncoding(String encoding) {
        this.encoding = encoding;
        if (executionContext != null) {
            executionContext.setEncoding(encoding);
        }
    }

    @Override
    public String getEncoding() {
        return this.encoding;
    }

    public void beginTransaction() {
        ITransaction trx = null;
        switch (trxConfig) {
            case COBAR_STYLE:
                trx = new CobarStyleTransaction(this.executionContext);
                break;
            case ALLOW_READ_CROSS_DB:
                trx = new AllowReadTransaction(this.executionContext);
                break;
            case AUTO_COMMIT:
                trx = new AutoCommitTransaction(this.executionContext);
                break;
            case STRICT:
                trx = new StrictlTransaction(this.executionContext);
                break;
        }

        this.trx = trx;
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        return trxPolicy;
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) throws TddlException {
        if (this.trxPolicy == trxPolicy) {
            return;
        }

        if (this.trx != null) {
            this.trx.commit();
            this.trx = null;
        }

        this.trxPolicy = trxPolicy;
    }

    /**
     * 最终清空缓存，无论是否在TStatement的时候清空了hint.
     */
    public void flush_hint() {
        flushOne(ThreadLocalString.ROUTE_CONDITION);
        flushOne(ThreadLocalString.DB_SELECTOR);
    }

    private void flushOne(String key) {
        RouteCondition rc = (RouteCondition) ThreadLocalMap.get(key);
        if (rc != null) {
            if (ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION.equals(rc.getRouteType())) {
                ThreadLocalMap.remove(key);
            }
        }
    }

    /**
     * 首先检查TXC_CONTEXT_STATE，存在则清理事务上下文
     */
    private void removeTxcContext() {
        Object value = ThreadLocalMap.get(ThreadLocalString.TXC_CONTEXT_MANAGER);
        if (value != null && value.equals(ThreadLocalString.TXC_MANAGER_NAME)) {
            ThreadLocalMap.remove(ThreadLocalString.TXC_CONTEXT);
            ThreadLocalMap.remove(ThreadLocalString.TXC_CONTEXT_MANAGER);
        }
    }

    @Override
    public void setMetaData(SqlMetaData sqlMetaData) {
    }

    @Override
    public SqlMetaData getSqlMetaData() {
        return null;
    }
}
