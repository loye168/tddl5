package com.alibaba.cobar.server.response;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.server.util.PacketUtil;

/**
 * 加入了offline状态推送，用于心跳语句。
 * 
 * @author xianmao.hexm 2012-4-28
 */
public class Ping {

    private static final ErrorPacket error = PacketUtil.getShutdown();

    public static void response(FrontendConnection c) {
        if (CobarServer.getInstance().isOnline()) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            error.write(c);
        }
    }

}
