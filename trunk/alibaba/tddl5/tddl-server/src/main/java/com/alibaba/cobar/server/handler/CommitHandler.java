package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.server.ServerConnection;

/**
 * @author xianmao.hexm
 */
public final class CommitHandler {

    public static void handle(ServerConnection c) {
        c.commit();
    }

}
