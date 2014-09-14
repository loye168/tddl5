package com.alibaba.cobar.net.packet;

/**
 * @author xianmao.hexm 2012-4-28
 */
public class PingPacket extends MySQLPacket {

    public static final byte[] PING = new byte[] { 1, 0, 0, 0, 14 };

    @Override
    protected String packetInfo() {
        return "MySQL Ping Packet";
    }

}
