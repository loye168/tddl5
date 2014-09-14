package com.alibaba.cobar.manager.response;

import java.nio.ByteBuffer;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.Fields;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.manager.parser.ManagerParseShow;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.util.LongUtil;
import com.alibaba.cobar.server.util.PacketUtil;

/**
 * @author xianmao.hexm 2010-9-27 下午02:53:11
 */
public final class ShowTime {

    private static final int                   FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[]         fields      = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket             eof         = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("TIMESTAMP", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, int type) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c);
        }

        // write eof
        buffer = eof.write(buffer, c);

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket row = getRow(type);
        row.packetId = ++packetId;
        buffer = row.write(buffer, c);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(int type) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        switch (type) {
            case ManagerParseShow.TIME_CURRENT:
                row.add(LongUtil.toBytes(System.currentTimeMillis()));
                break;
            case ManagerParseShow.TIME_STARTUP:
                row.add(LongUtil.toBytes(CobarServer.getInstance().getStartupTime()));
                break;
            default:
                row.add(LongUtil.toBytes(0L));
        }
        return row;
    }

}
