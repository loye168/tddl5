package com.alibaba.cobar.server.response;

import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.server.ServerConnection;

public class SqlMode {

    public static void response(String stmt, ServerConnection c, int rs) {
        String sqlMode = stmt.substring(rs >>> 8).trim();
        if (sqlMode.startsWith("'") || sqlMode.startsWith("`") || sqlMode.startsWith("\"")) {
            sqlMode = sqlMode.substring(1, sqlMode.length() - 1);
        }
        
        c.setSqlMode(sqlMode);
        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
    }
    
}
