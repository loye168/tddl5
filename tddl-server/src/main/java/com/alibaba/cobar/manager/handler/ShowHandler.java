package com.alibaba.cobar.manager.handler;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.manager.parser.ManagerParseShow;
import com.alibaba.cobar.manager.response.ShowCollation;
import com.alibaba.cobar.manager.response.ShowCommand;
import com.alibaba.cobar.manager.response.ShowConnection;
import com.alibaba.cobar.manager.response.ShowConnectionSQL;
import com.alibaba.cobar.manager.response.ShowDataSource;
import com.alibaba.cobar.manager.response.ShowDatabase;
import com.alibaba.cobar.manager.response.ShowHelp;
import com.alibaba.cobar.manager.response.ShowParser;
import com.alibaba.cobar.manager.response.ShowProcessor;
import com.alibaba.cobar.manager.response.ShowRouter;
import com.alibaba.cobar.manager.response.ShowSQL;
import com.alibaba.cobar.manager.response.ShowSQLDetail;
import com.alibaba.cobar.manager.response.ShowSQLExecute;
import com.alibaba.cobar.manager.response.ShowServer;
import com.alibaba.cobar.manager.response.ShowThreadPool;
import com.alibaba.cobar.manager.response.ShowTime;
import com.alibaba.cobar.manager.response.ShowVariables;
import com.alibaba.cobar.manager.response.ShowVersion;
import com.alibaba.cobar.server.util.ParseUtil;
import com.alibaba.cobar.server.util.StringUtil;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseShow.COMMAND:
                ShowCommand.execute(c);
                break;
            case ManagerParseShow.COLLATION:
                ShowCollation.execute(c);
                break;
            case ManagerParseShow.CONNECTION:
                ShowConnection.execute(c);
                break;
            case ManagerParseShow.CONNECTION_SQL:
                ShowConnectionSQL.execute(c);
                break;
            case ManagerParseShow.DATABASE:
                ShowDatabase.execute(c);
                break;
            case ManagerParseShow.DATASOURCE:
                ShowDataSource.execute(c, null);
                break;
            case ManagerParseShow.DATASOURCE_WHERE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    // ShowDataSource.execute(c, name);
                }
                break;
            }
            case ManagerParseShow.HELP:
                ShowHelp.execute(c);
                break;
            case ManagerParseShow.PARSER:
                ShowParser.execute(c);
                break;
            case ManagerParseShow.PROCESSOR:
                ShowProcessor.execute(c);
                break;
            case ManagerParseShow.ROUTER:
                ShowRouter.execute(c);
                break;
            case ManagerParseShow.SERVER:
                ShowServer.execute(c);
                break;
            case ManagerParseShow.SQL:
                ShowSQL.execute(c, ParseUtil.getSQLId(stmt));
                break;
            case ManagerParseShow.SQL_DETAIL:
                ShowSQLDetail.execute(c, ParseUtil.getSQLId(stmt));
                break;
            case ManagerParseShow.SQL_EXECUTE:
                ShowSQLExecute.execute(c);
                break;
            // case ManagerParseShow.SQL_SLOW:
            // ShowSQLSlow.execute(c);
            // break;

            case ManagerParseShow.THREADPOOL:
                ShowThreadPool.execute(c);
                break;
            case ManagerParseShow.TIME_CURRENT:
                ShowTime.execute(c, ManagerParseShow.TIME_CURRENT);
                break;
            case ManagerParseShow.TIME_STARTUP:
                ShowTime.execute(c, ManagerParseShow.TIME_STARTUP);
                break;
            case ManagerParseShow.VARIABLES:
                ShowVariables.execute(c);
                break;
            case ManagerParseShow.VERSION:
                ShowVersion.execute(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}
