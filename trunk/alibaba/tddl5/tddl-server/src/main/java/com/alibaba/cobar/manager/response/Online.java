/**
 * (created at 2011-11-22)
 */
package com.alibaba.cobar.manager.response;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.packet.OkPacket;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class Online {

    private static final OkPacket ok = new OkPacket();
    static {
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
    }

    public static void execute(String stmt, ManagerConnection mc) {
        CobarServer.getInstance().online();
        ok.write(mc);
    }

}
