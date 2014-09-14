package com.alibaba.cobar.net.packet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.net.FrontendConnection;

/**
 * @author mengshi.sunmengshi 2013-11-20 下午6:13:48
 * @since 5.1.0
 */
public class MysqlResultSetPacket extends MySQLPacket {

    public ResultSetHeaderPacket resulthead;
    public FieldPacket[]         fieldPackets;
    private List<RowDataPacket>  rowList;

    public synchronized void addRowDataPacket(RowDataPacket row) {
        if (rowList == null) {
            rowList = new ArrayList<RowDataPacket>();
        }
        rowList.add(row);
    }

    public int calcPacketSize() {
        return 4;
    }

    @Override
    protected String packetInfo() {
        return "ResultSet Packet";
    }

    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c) {
        byte packetId = 0;
        // write header
        resulthead.packetId = ++packetId;
        buffer = resulthead.write(buffer, c);
        // write fields

        if (this.fieldPackets != null) {
            for (FieldPacket field : this.fieldPackets) {
                field.packetId = ++packetId;
                buffer = field.write(buffer, c);
            }
        }
        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        buffer = eof.write(buffer, c);

        // row data
        if (this.rowList != null) {
            for (RowDataPacket row : this.rowList) {
                row.packetId = ++packetId;
                buffer = row.write(buffer, c);
            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);

        // write buffer
        c.write(buffer);

        return buffer;
    }

}
