package com.alibaba.cobar.server.response;

import java.nio.ByteBuffer;

import com.alibaba.cobar.Fields;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.util.LongUtil;
import com.alibaba.cobar.server.util.PacketUtil;
import com.alibaba.cobar.server.util.ParseUtil;

/**
 * @author xianmao.hexm
 */
public class SelectLastInsertId {

    private static final String                ORG_NAME    = "LAST_INSERT_ID()";
    private static final int                   FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    static {
        byte packetId = 0;
        header.packetId = ++packetId;
    }

    public static void response(ServerConnection c, String stmt, int aliasIndex) {
        String alias = ParseUtil.parseAlias(stmt, aliasIndex);
        if (alias == null) {
            alias = ORG_NAME;
        }

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c);

        // write fields
        byte packetId = header.packetId;
        FieldPacket field = PacketUtil.getField(alias, ORG_NAME, Fields.FIELD_TYPE_LONGLONG);
        field.packetId = ++packetId;
        buffer = field.write(buffer, c);

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        buffer = eof.write(buffer, c);

        // write rows
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(c.getLastInsertId()));
        row.packetId = ++packetId;
        buffer = row.write(buffer, c);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);

        // post write
        c.write(buffer);
    }

}
