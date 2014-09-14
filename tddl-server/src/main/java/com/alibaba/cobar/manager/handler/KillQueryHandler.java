package com.alibaba.cobar.manager.handler;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.util.StringUtil;
import com.taobao.tddl.common.utils.TStringUtil;

/**
 * @author xianmao.hexm 2012-4-17
 */
public class KillQueryHandler {

    public static void handle(String stmt, int offset, final ManagerConnection c) {
        String id = stmt.substring(offset).trim();
        if (StringUtil.isEmpty(id)) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
        } else {
            // get value
            long value = 0;
            try {
                value = Long.parseLong(id);
            } catch (NumberFormatException e) {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
                return;
            }

            // get the connection and kill query
            FrontendConnection fc = null;
            NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
            for (NIOProcessor p : processors) {
                if ((fc = p.getFrontends().get(value)) != null) {
                    break;
                }
            }
            if (fc != null && TStringUtil.equals(c.getUser(), fc.getUser())) {

                if (fc instanceof ServerConnection) {
                    ((ServerConnection) fc).cancel(c);
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
                        "Unknown command, Query to kill is not ServerConnection, but is " + fc.getClass().getName());
                }
            } else {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            }
        }
    }

}
