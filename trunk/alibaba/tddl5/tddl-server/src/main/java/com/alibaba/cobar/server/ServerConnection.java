package com.alibaba.cobar.server;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.config.SchemaConfig;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.net.util.ExecutorUtil;
import com.alibaba.cobar.net.util.NameableExecutor;
import com.alibaba.cobar.net.util.TimeUtil;
import com.alibaba.cobar.server.executor.utils.ResultSetUtil;
import com.alibaba.cobar.server.parser.ServerParse;
import com.alibaba.cobar.server.response.Ping;
import com.alibaba.cobar.server.session.ServerSession;
import com.alibaba.cobar.server.ugly.hint.HintRouter;
import com.alibaba.druid.pool.GetConnectionTimeoutException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.jdbc.ITransactionPolicy;
import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.matrix.jdbc.TPreparedStatement;
import com.taobao.tddl.matrix.jdbc.TResultSet;
import com.taobao.tddl.statistics.SQLRecorder;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm 2011-4-21 上午11:22:57
 */
public final class ServerConnection extends FrontendConnection {

    private static final Logger  logger       = LoggerFactory.getLogger(ServerConnection.class);
    private static final Logger  io_logger    = LoggerFactory.getLogger("net_error");
    private static final long    AUTH_TIMEOUT = 15 * 1000L;

    private volatile int         txIsolation  = -1;
    private volatile boolean     autocommit   = true;
    private ITransactionPolicy   trxPolicy    = ITransactionPolicy.ALLOW_READ_CROSS_DB;
    // empty sql mode
    private volatile String      sqlMode      = null;
    private ServerSession        session;
    private volatile TConnection conn;
    private NameableExecutor     localInfileExecutor;

    private long                 lastActiveTime;

    public ServerConnection(SocketChannel channel){
        super(channel);
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
        if (this.conn != null) {
            try {
                this.conn.setTransactionIsolation(txIsolation);
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
        if (this.conn != null) {
            try {
                this.conn.setAutoCommit(autocommit);
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    public long getLastInsertId() {
        return this.conn.getLastInsertId();
    }

    public ServerSession getSession() {
        return session;
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;

        if (this.conn != null) {
            this.conn.setSqlMode(sqlMode);
        }
    }

    @Override
    public boolean setCharsetIndex(int ci) {
        boolean result = super.setCharsetIndex(ci);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(charset);
            }
        }
        return result;
    }

    @Override
    public boolean setCharset(String charset) {
        boolean result = super.setCharset(charset);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(charset);
            }
        }

        return result;
    }

    @Override
    public void ping() {
        Ping.response(this);
    }

    public void execute(String sql, int type) {
        // 取得SCHEMA
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }
        lastActiveTime = TimeUtil.currentTimeMillis();
        ResultSet rs = null;
        TPreparedStatement stmt = null;

        ByteBuffer buffer = null;
        Exception ex = null;
        OkPacket ok = null;

        try {
            buildMDC();
            TConnection conn = getConnection(schema);
            // 兼容老的drds/cobar的hint,转化为新的tddl hint
            sql = HintRouter.convertHint(sql);
            stmt = conn.prepareStatement(sql);
            if (this.fileInputStream != null) {
                stmt.setLocalInfileInputStream(this.fileInputStream);
            }

            rs = stmt.executeQuery();

            // 先清掉标记为，防止前端在标记位还没清空的情况下发新的sql过来
            if (loadFile) {
                endLoadFile(false);
            }

            // 增删改的结果
            if (rs instanceof TResultSet && ((TResultSet) rs).getAffectRows() != -1) {
                ok = new OkPacket();
                ok.packetId = this.getNewPacketId();
                ok.insertId = conn.getLastInsertId();
                ok.affectedRows = ((TResultSet) rs).getAffectRows();
                ok.serverStatus = 2;

            } else {// 查询的结果
                buffer = ResultSetUtil.resultSetToPacket(rs, this.charset, this);
            }
        } catch (Exception e) {
            if (loadFile) {
                endLoadFile(true);
            }

            ex = e;

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Throwable e) {
                logger.error("", e);
            }

            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Throwable e) {
                logger.error("", e);
            }

            try {
                if (this.conn != null) {
                    if (this.conn.getExecutionContext() != null) {
                        if (this.conn.getExecutionContext().getTransaction() != null) {
                            this.conn.getExecutionContext().getTransaction().tryClose();
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("", e);
            }

            if (buffer != null) {
                recordSql(host, schema, sql);
                // 写最后的eof包
                ResultSetUtil.eofToPacket(buffer, this);
            } else if (ex != null) {
                handleError(ErrorCode.ERR_HANDLE_DATA, ex);
            } else if (ok != null) {
                recordSql(host, schema, sql);
                ok.write(this);
            }
        }
    }

    /**
     * 记录sql执行信息
     */
    private void recordSql(String host, SchemaConfig schema, String sql) {
        SQLRecorder sqlRecorder = schema.getRecorder();
        sqlRecorder.recordSql(sql, lastActiveTime, host, schema.getName());
        // if (sqlRecorder.check(time)) {
        // SQLRecord recorder = new SQLRecord();
        // recorder.host = host;
        // recorder.schema = schema.getName();
        // recorder.statement = sql;
        // recorder.startTime = lastActiveTime;
        // recorder.executeTime = time;
        //
        // sqlRecorder.add(recorder);
        // }
    }

    public void endLoadFile(boolean close) {
        this.loadDataSql = null;
        this.loadFile = false;

        if (fileInputStream != null) {
            try {
                if (close) {
                    while (fileInputStream.read() != -1) {
                        ;
                    }
                }
            } catch (IOException e) {
                logger.error("error when close load file fileInputStream", e);
            } finally {
                fileInputStream = null;

            }

        }

    }

    /**
     * 提交事务
     */
    public void commit() {
        try {
            if (this.conn != null) {
                conn.commit();
            }

            ByteBuffer buffer = this.allocate();
            this.write(this.writeToBuffer(OkPacket.OK, buffer));
        } catch (Exception ex) {
            this.handleError(ErrorCode.ERR_HANDLE_DATA, ex);
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {

        try {
            if (this.conn != null) {
                conn.rollback();
            }

            ByteBuffer buffer = this.allocate();
            this.write(this.writeToBuffer(OkPacket.OK, buffer));
        } catch (Exception ex) {
            this.handleError(ErrorCode.ERR_HANDLE_DATA, ex);
        }
    }

    @Override
    public void handleError(int errCode, Throwable t) {
        Throwable ex = t;
        String message = null;
        List<Throwable> ths = ExceptionUtils.getThrowableList(t);
        for (int i = ths.size() - 1; i >= 0; i--) {
            Throwable e = ths.get(i);
            if (GetConnectionTimeoutException.class.isInstance(e)) {
                if (e.getCause() != null) {
                    message = e.getCause().getMessage();
                } else {
                    message = e.getMessage();
                }

                break;
            } else if (SQLSyntaxErrorException.class.isInstance(e)) {
                errCode = ErrorCode.ER_PARSE_ERROR;
                ex = e;
                break;
            }

        }

        if (message == null) {
            message = t.getMessage();
        }

        // 根据异常类型和信息，选择日志输出级别。
        if (this.schema == null) {
            // DNS探测日志的schema一定为null,正常应用访问日志schema绝大多数情况下不为null
            io_logger.error(toString(), t);
        } else {
            if (ex instanceof EOFException) {
                if (logger.isInfoEnabled()) {
                    logger.info(ex);
                }
            } else if (isConnectionReset(ex)) {
                if (logger.isInfoEnabled()) {
                    logger.info(ex);
                }
            } else {
                logger.warn(ex);
            }
        }

        switch (errCode) {
            case ErrorCode.ERR_HANDLE_DATA:
                writeErrMessage(ErrorCode.ER_YES, message == null ? t.getClass().getSimpleName() : message);
                break;
            case ErrorCode.ER_PARSE_ERROR:
                writeErrMessage(ErrorCode.ER_PARSE_ERROR, message == null ? t.getClass().getSimpleName() : message);
                break;
            default:
                close();
        }
    }

    @Override
    public boolean close() {
        if (super.close()) {
            if (this.conn != null) {
                // 单独线程池做kill操作,防止SQL执行的线程池满,kill操作挂起的情况出现
                processor.getKillExecutor().execute(new Runnable() {

                    @Override
                    public void run() {
                        buildMDC();
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            logger.warn("error when close", e);
                        }
                    }
                });
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 撤销执行中的语句
     * 
     * @param sponsor 发起者为null表示是自己
     */
    public void cancel(final FrontendConnection sponsor) {
        processor.getKillExecutor().execute(new Runnable() {

            @Override
            public void run() {
                buildMDC();
                writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "Query execution was interrupted");

                try {
                    conn.cancelQuery();
                } catch (Exception ex) {
                    logger.warn("error when kill", ex);

                }

                if (sponsor != null) {
                    OkPacket packet = new OkPacket();
                    packet.packetId = 1;
                    packet.affectedRows = 0;
                    packet.serverStatus = 2;
                    packet.write(sponsor);
                }
            }
        });
    }

    public void kill() {
        if (super.close()) {
            if (this.conn != null) {

                // 单独线程池做kill操作,防止SQL执行的线程池满,kill操作挂起的情况出现
                processor.getKillExecutor().execute(new Runnable() {

                    @Override
                    public void run() {
                        buildMDC();
                        try {
                            conn.kill();
                        } catch (Exception ex) {
                            logger.warn("error when kill", ex);
                        } finally {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                logger.warn("error when close", e);
                            }
                        }
                    }
                });
            }
        }
    }

    private TConnection getConnection(SchemaConfig schema) throws TddlException, SQLException {
        if (this.conn == null) {
            synchronized (this) {
                if (this.conn == null) {// double-check
                    TDataSource ds = schema.getDataSource();
                    if (!ds.isInited()) {
                        ds.init();
                    }
                    this.conn = ds.getConnection();
                    if (txIsolation >= 0) {
                        this.conn.setTransactionIsolation(txIsolation);
                    }

                    if (!autocommit) {
                        this.conn.setAutoCommit(autocommit);
                    }

                    if (sqlMode != null) {
                        this.conn.setSqlMode(sqlMode);
                    }

                    if (charset != null) {
                        this.conn.setEncoding(charset);
                    }

                    if (this.trxPolicy != null) {
                        this.conn.setTrxPolicy(this.trxPolicy);
                    }
                }
            }
        }

        return this.conn;
    }

    public TConnection getTddlConnection() {
        return this.conn;
    }

    @Override
    protected void prepareLoadInfile() {
        if (this.localInfileExecutor == null) {
            this.localInfileExecutor = ExecutorUtil.create("PREPARE_LOAD_INFILE_THREAD", 1, true);
        }

        this.localInfileExecutor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    buildMDC();
                    execute(loadDataSql, ServerParse.LOAD);
                } catch (Throwable e) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, e);
                } finally {

                }
            }
        });
        return;
    }

    public void setTrxPolicy(ITransactionPolicy policy) {
        this.trxPolicy = policy;
        if (this.conn != null) {
            try {
                this.conn.setTrxPolicy(policy);
                this.write(this.writeToBuffer(OkPacket.OK, this.allocate()));
            } catch (TddlException e) {
                handleError(ErrorCode.ERR_HANDLE_DATA, e);
            }
        } else {
            this.write(this.writeToBuffer(OkPacket.OK, this.allocate()));
        }
    }

}
