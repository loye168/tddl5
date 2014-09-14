package com.taobao.tddl.group.jdbc;

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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.atom.jdbc.TConnectionWrapper;
import com.taobao.tddl.atom.utils.EncodingUtils;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.ITransactionPolicy;
import com.taobao.tddl.common.jdbc.TExceptionUtils;
import com.taobao.tddl.common.model.SqlMetaData;
import com.taobao.tddl.group.config.GroupIndex;
import com.taobao.tddl.group.dbselector.DBSelector;
import com.taobao.tddl.group.dbselector.DBSelector.AbstractDataSourceTryer;
import com.taobao.tddl.group.dbselector.DBSelector.DataSourceTryer;
import com.taobao.tddl.group.utils.GroupHintParser;
import com.taobao.tddl.monitor.unit.RouterUnitsHelper;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * <pre>
 * 相关的JDBC规范： 
 * 1. Connection关闭，在其上打开的statement自动关闭。这就要求Connection持有其上打开的所有statement的引用 
 * 2. 重试的场景
 *    1：在第一个statement上执行查询，路由到db1成功。再创建一个statement查询在db1上失败： 
 *    stmt1 = TGroupConnection.createStatement 
 *    rs1 = stmt1.executeQuery 
 *    --create connection on db1 and execute success 
 *    stmt2 = conn..createStatement 
 *    rs2 = stmt2..executeQuery 
 *    --db1 failed then... 
 *    这时如果重试到db2库，db1的connection要不要关？
 *    a：如果关，其上的实际stmt和rs就都会关掉。这样db2成功后用户会看不到exception，对用户来说，stms1和rs1都是正常的。但实际上已经是坏掉的了。
 *    b: 如果不关，也就是TGroupConnection持有多个baseConnection，。。。 
 *    
 *    由以上场景的考虑，提炼出一个重试的原则： 
 *    a. 一个TGroupConnection中，只在第一次与真正与数据库交互时，也就是不得不返回db结果给用户时，才在DBGroup上进行重试。
 *    b. 一旦在某个库上重试成功，后续在这个TGroupConnection上执行的所有操作，都只到这个库上，不再重试，出错直接抛出异常。
 *    c. 第一次建立真正连接的重试过程中，baseConnection有可能会发生变化被替换。一旦重试成功，baseConnection则保持不再改变。
 *    
 * 这样可以简化很多事情，但同时不会对功能造成本质影响。同时避免了对状态处理不当，可能会给用户造成的诡异现象。
 * </pre>
 * 
 * @author linxuan
 * @author yangzhu
 */
public class TGroupConnection implements IConnection {

    private static final Logger log = LoggerFactory.getLogger(TGroupConnection.class);

    private TGroupDataSource    tGroupDataSource;
    private SqlMetaData         sqlMetaData;

    // 虽然DataSource.getConnection(String username, String password)不常用，
    // 但为了尽量遵循jdbc规范，还是保留好。
    private String              username;
    private String              password;

    public TGroupConnection(TGroupDataSource tGroupDataSource){
        this.tGroupDataSource = tGroupDataSource;
    }

    public TGroupConnection(TGroupDataSource tGroupDataSource, String username, String password){
        this(tGroupDataSource);
        this.username = username;
        this.password = password;
    }

    /*
     * ========================================================================
     * 下层connection的持有，getter/setter包权限
     * ======================================================================
     */
    private Connection             rBaseConnection;
    private Connection             wBaseConnection;
    // private String rBaseDsKey; // rBaseConnection对应的数据源key
    // private String wBaseDsKey; // wBaseConnection对应的数据源key
    // private int rBaseDataSourceIndex = -2; // rBaseConnection对应的数据源Index
    // private int wBaseDataSourceIndex = -2; // wBaseConnection对应的数据源Index
    private DataSourceWrapper      rBaseDsWrapper;
    private DataSourceWrapper      wBaseDsWrapper;
    private Set<TGroupStatement>   openedStatements     = Collections.synchronizedSet(new HashSet<TGroupStatement>(2));
    private int                    transactionIsolation = -1;
    private String                 encoding             = null;
    private String                 sqlMode              = null;

    public static final GroupIndex DEFAULT_GROUPINDEX   = new GroupIndex(DBSelector.NOT_EXIST_USER_SPECIFIED_INDEX,
                                                            false);

    /**
     * 获取事务中的上一个操作的链接
     */
    Connection getBaseConnection(String sql, boolean isRead) throws SQLException {
        GroupIndex dataSourceIndex = DEFAULT_GROUPINDEX;
        if (sql == null) {
            // 如果当前的数据源索引与上一次的数据源索引不一样，说明上一次缓存的Connection已经无用了，需要关闭后重建。
            dataSourceIndex = ThreadLocalDataSourceIndex.getIndex();
        } else {
            dataSourceIndex = GroupHintParser.convertHint2Index(sql);
            if (dataSourceIndex == null) {
                dataSourceIndex = ThreadLocalDataSourceIndex.getIndex();
            }
        }

        // 代表出现自定义index请求
        if (dataSourceIndex.index != DBSelector.NOT_EXIST_USER_SPECIFIED_INDEX) {
            if (log.isDebugEnabled()) {
                log.debug("dataSourceIndex=" + dataSourceIndex);
            }
            // 在事务状态下，设置不同的数据源索引会导致异常。
            if (!isAutoCommit) {
                if (wBaseDsWrapper != null && !wBaseDsWrapper.isMatchDataSourceIndex(dataSourceIndex.index)) {
                    throw new SQLException("Transaction in another dataSourceIndex: " + dataSourceIndex);
                }
            }

            if (isRead) {
                if (rBaseDsWrapper != null && !rBaseDsWrapper.isMatchDataSourceIndex(dataSourceIndex.index)) {
                    closeReadConnection();
                }
            } else {
                if (wBaseDsWrapper != null && !wBaseDsWrapper.isMatchDataSourceIndex(dataSourceIndex.index)) {
                    closeWriteConnection();
                }
            }
        }

        // 为了保证事务正确关闭，在事务状态下只会取回写连接
        if (isRead && isAutoCommit) {
            // 只要有写连接，并且对应的库可读，则复用。否则返回读连接
            return wBaseConnection != null && wBaseDsWrapper.hasReadWeight() ? wBaseConnection : rBaseConnection;
            // 先写后读，重用写连接读后，rBaseConnection仍然是null
        } else {
            if (wBaseConnection != null) {
                this.tGroupDataSource.setWriteTarget(wBaseDsWrapper);
                return wBaseConnection;
            } else if (rBaseConnection != null && rBaseDsWrapper.hasWriteWeight()) {
                // 在写连接null的情况下，如果读连接已经建立，且对应的库可写，则复用
                wBaseConnection = rBaseConnection; // wBaseConnection赋值，以确保事务能够正确提交回滚
                // 在写连接上设置事务
                if (wBaseConnection.getAutoCommit() != isAutoCommit) {
                    wBaseConnection.setAutoCommit(isAutoCommit);
                }
                // wBaseDsKey = rBaseDsKey;
                wBaseDsWrapper = rBaseDsWrapper;
                this.tGroupDataSource.setWriteTarget(wBaseDsWrapper);
                return wBaseConnection;
            } else {
                return null;
            }
        }
    }

    /**
     * 从实际的DataSource获得一个下层（有可能不是真实的）Connection
     * 包权限：此方法只在TGroupStatement、TGroupPreparedStatement中使用
     */
    Connection createNewConnection(DataSourceWrapper dsw, boolean isRead) throws SQLException {
        // 这个方法只发生在第一次建立读/写连接的时候，以后都是复用了
        Connection conn;
        if (username != null) {
            conn = dsw.getConnection(username, password);
        } else {
            conn = dsw.getConnection();
        }

        if (sqlMetaData != null) {
            if (conn instanceof TConnectionWrapper) {
                ((TConnectionWrapper) conn).setSqlMetaData(sqlMetaData);
            }
        }

        // 为了保证事务正确关闭，在事务状态下只设置写连接
        setBaseConnection(conn, dsw, isRead && isAutoCommit);

        // 只在写连接上调用 setAutoCommit, 与 TGroupConnection#setAutoCommit 的代码保持一致
        if (!isRead || !isAutoCommit) {
            conn.setAutoCommit(isAutoCommit); // 新建连接的AutoCommit要与当前isAutoCommit的状态同步
        }

        if (transactionIsolation >= 0) {
            if (transactionIsolation != conn.getTransactionIsolation()) {
                conn.setTransactionIsolation(transactionIsolation);
            }
        }

        if (StringUtils.isNotEmpty(encoding)) {
            String connEncoding = EncodingUtils.getEncoding(conn);
            String mysqlEncoding = EncodingUtils.mysqlEncoding(encoding);
            String javaEncoding = EncodingUtils.javaEncoding(encoding);
            if (!(StringUtils.equalsIgnoreCase(connEncoding, encoding) || StringUtils.equalsIgnoreCase(connEncoding,
                mysqlEncoding))) {
                // 设置编码
                EncodingUtils.setEncoding(conn, javaEncoding);
                Statement stmt = conn.createStatement();
                stmt.execute("set names " + mysqlEncoding);
            }
        }

        if (sqlMode != null) {
            Statement stmt = conn.createStatement();
            stmt.execute("set sql_mode=\"" + sqlMode + "\"");
        }

        return conn;
    }

    private void setBaseConnection(Connection baseConnection, DataSourceWrapper dsw, boolean isRead) {
        if (baseConnection == null) {
            log.warn("setBaseConnection to null !!");
        }

        if (isRead) {
            closeReadConnection();
        } else {
            closeWriteConnection();
        }

        if (isRead) {
            rBaseConnection = baseConnection;
            // this.rBaseDsKey = dsw.getDataSourceKey();
            // this.rBaseDataSourceIndex = dsw.getDataSourceIndex();
            this.rBaseDsWrapper = dsw;
        } else {
            wBaseConnection = baseConnection;
            // this.wBaseDsKey = dsw.getDataSourceKey();
            // this.wBaseDataSourceIndex = dsw.getDataSourceIndex();
            this.wBaseDsWrapper = dsw;
            this.tGroupDataSource.setWriteTarget(dsw);
        }
    }

    private void closeReadConnection() {
        // r|wBaseConnection可能指向同一个对象，如果另一个引用在用，就不去关闭
        if (rBaseConnection != null && rBaseConnection != wBaseConnection) {
            try {
                rBaseConnection.close(); // 旧的baseConnection要关闭
            } catch (SQLException e) {
                log.error("close rBaseConnection failed.", e);
            }
            rBaseDsWrapper = null;
            rBaseConnection = null;
        }
    }

    private void closeWriteConnection() {
        // r|wBaseConnection可能指向同一个对象，如果另一个引用在用，就不去关闭
        if (wBaseConnection != null && rBaseConnection != wBaseConnection) {
            try {
                wBaseConnection.close(); // 旧的baseConnection要关闭
            } catch (SQLException e) {
                log.error("close wBaseConnection failed.", e);
            }
            wBaseDsWrapper = null;
            wBaseConnection = null;
        }
    }

    void removeOpenedStatements(Statement statement) {
        if (!openedStatements.remove(statement)) {
            log.warn("current statmenet ：" + statement + " doesn't exist!");
        }
    }

    /*
     * ========================================================================
     * 关闭逻辑
     * ======================================================================
     */
    private boolean closed;

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
    @SuppressWarnings("unchecked")
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {
            // 关闭statement
            for (TGroupStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }

            try {
                if (rBaseConnection != null && !rBaseConnection.isClosed()) {
                    rBaseConnection.close();
                }
            } catch (SQLException e) {
                exceptions.add(e);
            }
            try {
                if (wBaseConnection != null && !wBaseConnection.isClosed()) {
                    wBaseConnection.close();
                }
            } catch (SQLException e) {
                exceptions.add(e);
            }
        } finally {
            openedStatements.clear();
            // openedStatements = null; //逻辑完整性
            rBaseConnection = null;
            wBaseConnection = null;

            ThreadLocalDataSourceIndex.clearIndex();
            RouterUnitsHelper.clearUnitValidThreadLocal();
        }
        TExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    /*
     * ========================================================================
     * 创建Statement逻辑
     * ======================================================================
     */
    @Override
    public TGroupStatement createStatement() throws SQLException {
        checkClosed();
        TGroupStatement stmt = new TGroupStatement(tGroupDataSource, this, this.tGroupDataSource.getAppName());
        openedStatements.add(stmt);
        return stmt;
    }

    @Override
    public TGroupStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        TGroupStatement stmt = createStatement();
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public TGroupStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                                 throws SQLException {
        TGroupStatement stmt = createStatement(resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    /*
     * ========================================================================
     * 创建PreparedStatement逻辑
     * ======================================================================
     */
    @Override
    public TGroupPreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        TGroupPreparedStatement stmt = new TGroupPreparedStatement(tGroupDataSource,
            this,
            sql,
            this.tGroupDataSource.getAppName());
        openedStatements.add(stmt);
        return stmt;
    }

    @Override
    public TGroupPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                            throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public TGroupPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                    int resultSetHoldability) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql, resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    @Override
    public TGroupPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setAutoGeneratedKeys(autoGeneratedKeys);
        return stmt;
    }

    @Override
    public TGroupPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnIndexes(columnIndexes);
        return stmt;
    }

    @Override
    public TGroupPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnNames(columnNames);
        return stmt;
    }

    /*
     * ========================================================================
     * 创建CallableStatement逻辑。存储过程CallableStatement支持
     * ======================================================================
     */
    private DataSourceTryer<CallableStatement> getCallableStatementTryer = new AbstractDataSourceTryer<CallableStatement>() {

                                                                             @Override
                                                                             public CallableStatement tryOnDataSource(DataSourceWrapper dsw,
                                                                                                                      Object... args)
                                                                                                                                     throws SQLException {
                                                                                 String sql = (String) args[0];
                                                                                 int resultSetType = (Integer) args[1];
                                                                                 int resultSetConcurrency = (Integer) args[2];
                                                                                 int resultSetHoldability = (Integer) args[3];
                                                                                 Connection conn = TGroupConnection.this.createNewConnection(dsw,
                                                                                     false);
                                                                                 return getCallableStatement(conn,
                                                                                     sql,
                                                                                     resultSetType,
                                                                                     resultSetConcurrency,
                                                                                     resultSetHoldability);
                                                                             }
                                                                         };

    private CallableStatement getCallableStatement(Connection conn, String sql, int resultSetType,
                                                   int resultSetConcurrency, int resultSetHoldability)
                                                                                                      throws SQLException {
        if (resultSetType == Integer.MIN_VALUE) {
            return conn.prepareCall(sql);
        } else if (resultSetHoldability == Integer.MIN_VALUE) {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
        } else {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }

    @Override
    public TGroupCallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        checkClosed();
        CallableStatement target;

        Connection conn = this.getBaseConnection(sql, false); // 存储过程默认走写库
        if (conn != null) {
            sql = GroupHintParser.removeTddlGroupHint(sql);
            target = getCallableStatement(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } else {
            // hint优先
            GroupIndex dataSourceIndex = GroupHintParser.convertHint2Index(sql);
            sql = GroupHintParser.removeTddlGroupHint(sql);
            if (dataSourceIndex == null) {
                dataSourceIndex = ThreadLocalDataSourceIndex.getIndex();
            }
            target = tGroupDataSource.getDBSelector(false).tryExecute(null,
                getCallableStatementTryer,
                this.tGroupDataSource.getRetryingTimes(),
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                dataSourceIndex);
        }

        TGroupCallableStatement stmt = new TGroupCallableStatement(tGroupDataSource,
            this,
            target,
            sql,
            this.tGroupDataSource.getAppName());
        if (resultSetType != Integer.MIN_VALUE) {
            stmt.setResultSetType(resultSetType);
            stmt.setResultSetConcurrency(resultSetConcurrency);
        }
        if (resultSetHoldability != Integer.MIN_VALUE) {
            stmt.setResultSetHoldability(resultSetHoldability);
        }
        openedStatements.add(stmt);
        return stmt;
    }

    @Override
    public TGroupCallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public TGroupCallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                       throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, Integer.MIN_VALUE);
    }

    /*
     * ========================================================================
     * JDBC事务相关的autoCommit设置、commit/rollback、TransactionIsolation等
     * ======================================================================
     */
    private boolean isAutoCommit = true; // jdbc规范，新连接为true

    @Override
    public void setAutoCommit(boolean autoCommit0) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit0) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit0;
        /*
         * /////////////////////////////////////只读情况忽略事务 if
         * (this.rBaseConnection != null) {
         * this.rBaseConnection.setAutoCommit(autoCommit0); }
         */
        if (this.wBaseConnection != null) {
            this.wBaseConnection.setAutoCommit(autoCommit0);
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
        if (isAutoCommit) {
            return;
        }

        /*
         * /////////////////////////////////////只读情况忽略事务 if (rBaseConnection !=
         * null) { try { rBaseConnection.commit(); } catch (SQLException e) {
         * log.error("Commit failed on " + this.rBaseDsKey + ":" +
         * e.getMessage()); throw e; } }
         */
        if (wBaseConnection != null) {
            try {
                wBaseConnection.commit();
            } catch (SQLException e) {
                log.error("Commit failed on " + this.wBaseDsWrapper.getDataSourceKey() + ":" + e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        /*
         * /////////////////////////////////////只读情况忽略事务 if (rBaseConnection !=
         * null) { try { rBaseConnection.rollback(); } catch (SQLException e) {
         * log.error("Rollback failed on " + this.rBaseDsKey + ":" +
         * e.getMessage()); throw e; } }
         */

        if (wBaseConnection != null) {
            try {
                wBaseConnection.rollback();
            } catch (SQLException e) {
                log.error("Rollback failed on " + this.wBaseDsWrapper.getDataSourceKey() + ":" + e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public void setTransactionIsolation(int transactionIsolation) throws SQLException {
        checkClosed();
        if (this.transactionIsolation == transactionIsolation) {
            return;
        }

        this.transactionIsolation = transactionIsolation;
        if (this.rBaseConnection != null) {
            this.rBaseConnection.setTransactionIsolation(transactionIsolation);
        }

        if (this.wBaseConnection != null) {
            this.wBaseConnection.setTransactionIsolation(transactionIsolation);
        }
    }

    /*
     * ========================================================================
     * SQLWarning 和 DatabaseMetaData
     * ======================================================================
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        if (rBaseConnection != null) {
            return rBaseConnection.getWarnings();
        } else if (wBaseConnection != null) {
            return wBaseConnection.getWarnings();
        } else {
            return null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        if (rBaseConnection != null) {
            rBaseConnection.clearWarnings();
        }
        if (wBaseConnection != null) {
            wBaseConnection.clearWarnings();
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        if (rBaseConnection != null) {
            return rBaseConnection.getMetaData();
        } else if (wBaseConnection != null) {
            return wBaseConnection.getMetaData();
        } else {
            return new TGroupDatabaseMetaData(this, tGroupDataSource);
        }
    }

    /*
     * ========================================================================
     * 后面是未实现的方法
     * ======================================================================
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        /*
         * 如果你看到这里，那么恭喜，哈哈 mysql默认在5.x的jdbc driver里面也没有实现holdability 。
         * 所以默认都是.CLOSE_CURSORS_AT_COMMIT 为了简化起见，我们也就只实现close这种
         */

        // mysql 5.x的jdbc driver只支持ResultSet.HOLD_CURSORS_OVER_COMMIT
        throw new UnsupportedOperationException("setHoldability");
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

    /**
     * 保持可读可写
     * 
     * @author junyu
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /**
     * 不做任何事情
     * 
     * @author junyu
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
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
    public boolean isValid(int timeout) throws SQLException {
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
    public void cancelQuery() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {
            // 关闭statement
            for (TGroupStatement stmt : openedStatements) {
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }

        } finally {
            openedStatements.clear();
        }
        TExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    @Override
    public void kill() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {
            // cancel调现在的所有查询
            try {
                this.cancelQuery();
            } catch (SQLException e) {
                exceptions.add(e);
            }

        } finally {
            try {
                this.close();
            } catch (SQLException e) {
                exceptions.add(e);
            }
        }
        TExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    @Override
    public long getLastInsertId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastInsertId(long id) {
        throw new UnsupportedOperationException();

    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) {
        throw new UnsupportedOperationException();

    }

    @Override
    public List<Long> getGeneratedKeys() {
        return new ArrayList<Long>();
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
    }

    @Override
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    @Override
    public String getSqlMode() {
        return sqlMode;
    }

    @Override
    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    @Override
    public void setMetaData(SqlMetaData sqlMetaData) {
        this.sqlMetaData = sqlMetaData;
    }

    @Override
    public SqlMetaData getSqlMetaData() {
        return sqlMetaData;
    }

}
