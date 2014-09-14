package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.server.ServerConnection;

/**
 * @author xianmao.hexm
 */
public final class BeginHandler {

    public static void handle(String stmt, ServerConnection c) {
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
    }

}
