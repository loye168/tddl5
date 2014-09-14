package com.alibaba.cobar.net.packet;

/**
 * @author xianmao.hexm
 */
public abstract class MySQLPacket {

    public int  packetLength;
    public byte packetId;

    protected abstract String packetInfo();

    @Override
    public String toString() {
        return new StringBuilder().append(packetInfo())
            .append("{length=")
            .append(packetLength)
            .append(",id=")
            .append(packetId)
            .append('}')
            .toString();
    }

}
