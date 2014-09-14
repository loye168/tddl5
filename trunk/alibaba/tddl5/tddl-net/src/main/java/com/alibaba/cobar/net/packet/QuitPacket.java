package com.alibaba.cobar.net.packet;

/**
 * @author xianmao.hexm
 */
public class QuitPacket extends MySQLPacket {

    public static final byte[] QUIT = new byte[] { 1, 0, 0, 0, 1 };

    @Override
    protected String packetInfo() {
        return "MySQL Quit Packet";
    }

}
