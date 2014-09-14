package com.alibaba.cobar.manager;

import java.io.EOFException;
import java.nio.channels.SocketChannel;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.util.TimeUtil;
import com.alibaba.druid.pool.GetConnectionTimeoutException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm 2011-4-22 下午02:23:55
 */
public final class ManagerConnection extends FrontendConnection {

    private static final Logger logger       = LoggerFactory.getLogger(ManagerConnection.class);
    private static final long   AUTH_TIMEOUT = 15 * 1000L;

    public ManagerConnection(SocketChannel channel){
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

    @Override
    public void handleData(final byte[] data) {
        CobarServer.getInstance().getManagerExecutor().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    handler.handle(data);
                } catch (Throwable e) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, e);
                }
            }
        });
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
        switch (errCode) {
            case ErrorCode.ERR_HANDLE_DATA:
                String msg = t.getMessage();
                writeErrMessage(ErrorCode.ER_YES, msg == null ? t.getClass().getSimpleName() : msg);
                break;
            default:
                close();
        }
    }

    @Override
    protected void prepareLoadInfile() {
        writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "handle load file is not supported in ManagerConnection");
    }

}
