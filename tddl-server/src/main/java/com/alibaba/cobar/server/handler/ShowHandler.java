package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.parser.ServerParse;
import com.alibaba.cobar.server.parser.ServerParseShow;
import com.alibaba.cobar.server.response.ShowConnection;
import com.alibaba.cobar.server.response.ShowDatabases;
import com.alibaba.cobar.server.response.ShowPhysicalSQLSlow;
import com.alibaba.cobar.server.response.ShowSQLSlow;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    public static void handle(String stmt, ServerConnection c, int offset) {
        switch (ServerParseShow.parse(stmt, offset)) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;

            case ServerParseShow.CONNECTION:
                ShowConnection.execute(c);
                break;
            //            case ServerParseShow.DATASOURCES:
            //                // ShowDataSources.response(c);
            //                // break;
            //            case ServerParseShow.COBAR_STATUS:
            //                // ShowCobarStatus.response(c);
            //                // break;
            case ServerParseShow.SLOW:
                ShowSQLSlow.execute(c);
                break;
            case ServerParseShow.PHYSICAL_SLOW:
                ShowPhysicalSQLSlow.execute(c);
                break;
            default:
                c.execute(stmt, ServerParse.SHOW);
        }
    }

}
