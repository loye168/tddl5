package com.alibaba.cobar.server.session;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.server.ServerConnection;

/**
 * 由前后端参与的一次执行会话过程
 * 
 * @author xianmao.hexm
 */
public final class ServerSession {

    private final ServerConnection source;

    public ServerSession(ServerConnection source){
        this.source = source;

    }

    public ServerConnection getSource() {
        return source;
    }

    /**
     * 撤销执行中的会话
     */
    public void cancel(FrontendConnection sponsor) {
        // TODO terminate session
        source.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "Query execution was interrupted");
        if (sponsor != null) {
            OkPacket packet = new OkPacket();
            packet.packetId = 1;
            packet.affectedRows = 0;
            packet.serverStatus = 2;
            packet.write(sponsor);
        }
    }

}
