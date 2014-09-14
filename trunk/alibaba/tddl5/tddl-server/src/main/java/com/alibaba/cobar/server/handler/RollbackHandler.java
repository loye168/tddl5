package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.server.ServerConnection;

/**
 * @author xianmao.hexm
 */
public final class RollbackHandler {

    public static void handle(String stmt, ServerConnection c) {
        c.rollback();
    }

}
