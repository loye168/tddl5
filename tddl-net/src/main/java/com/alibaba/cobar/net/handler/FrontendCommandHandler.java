package com.alibaba.cobar.net.handler;

import com.alibaba.cobar.Commands;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.FrontendConnection;

/**
 * 前端命令处理器
 * 
 * @author xianmao.hexm
 */
public class FrontendCommandHandler implements NIOHandler {

    protected final FrontendConnection source;
    protected final CommandCount       commandCount;

    public FrontendCommandHandler(FrontendConnection source){
        this.source = source;
        this.commandCount = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        source.setPacketId((byte) (0 & 0xff));
        switch (data[4]) {
            case Commands.COM_INIT_DB:
                commandCount.doInitDB();
                source.initDB(data);
                break;
            case Commands.COM_QUERY:
                commandCount.doQuery();
                source.query(data);
                break;
            case Commands.COM_PING:
                commandCount.doPing();
                source.ping();
                break;
            case Commands.COM_QUIT:
                commandCount.doQuit();
                source.close();
                break;
            case Commands.COM_PROCESS_KILL:
                commandCount.doKill();
                source.kill(data);
                break;
            case Commands.COM_STMT_PREPARE:
                commandCount.doStmtPrepare();
                source.stmtPrepare(data);
                break;
            case Commands.COM_STMT_EXECUTE:
                commandCount.doStmtExecute();
                source.stmtExecute(data);
                break;
            case Commands.COM_STMT_CLOSE:
                commandCount.doStmtClose();
                source.stmtClose(data);
                break;
            default:
                commandCount.doOther();
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

}
