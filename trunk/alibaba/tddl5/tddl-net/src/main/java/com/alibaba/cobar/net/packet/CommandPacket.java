package com.alibaba.cobar.net.packet;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.alibaba.cobar.net.BackendConnection;
import com.alibaba.cobar.net.util.BufferUtil;
import com.alibaba.cobar.net.util.MySQLMessage;
import com.alibaba.cobar.net.util.StreamUtil;

/**
 * From client to server whenever the client wants the server to do something.
 * 
 * <pre>
 * Bytes         Name
 * -----         ----
 * 1             command
 * n             arg
 * 
 * command:      The most common value is 03 COM_QUERY, because
 *               INSERT UPDATE DELETE SELECT etc. have this code.
 *               The possible values at time of writing (taken
 *               from /include/mysql_com.h for enum_server_command) are:
 * 
 *               #      Name                Associated client function
 *               -      ----                --------------------------
 *               0x00   COM_SLEEP           (none, this is an internal thread state)
 *               0x01   COM_QUIT            mysql_close
 *               0x02   COM_INIT_DB         mysql_select_db 
 *               0x03   COM_QUERY           mysql_real_query
 *               0x04   COM_FIELD_LIST      mysql_list_fields
 *               0x05   COM_CREATE_DB       mysql_create_db (deprecated)
 *               0x06   COM_DROP_DB         mysql_drop_db (deprecated)
 *               0x07   COM_REFRESH         mysql_refresh
 *               0x08   COM_SHUTDOWN        mysql_shutdown
 *               0x09   COM_STATISTICS      mysql_stat
 *               0x0a   COM_PROCESS_INFO    mysql_list_processes
 *               0x0b   COM_CONNECT         (none, this is an internal thread state)
 *               0x0c   COM_PROCESS_KILL    mysql_kill
 *               0x0d   COM_DEBUG           mysql_dump_debug_info
 *               0x0e   COM_PING            mysql_ping
 *               0x0f   COM_TIME            (none, this is an internal thread state)
 *               0x10   COM_DELAYED_INSERT  (none, this is an internal thread state)
 *               0x11   COM_CHANGE_USER     mysql_change_user
 *               0x12   COM_BINLOG_DUMP     sent by the slave IO thread to request a binlog
 *               0x13   COM_TABLE_DUMP      LOAD TABLE ... FROM MASTER (deprecated)
 *               0x14   COM_CONNECT_OUT     (none, this is an internal thread state)
 *               0x15   COM_REGISTER_SLAVE  sent by the slave to register with the master (optional)
 *               0x16   COM_STMT_PREPARE    mysql_stmt_prepare
 *               0x17   COM_STMT_EXECUTE    mysql_stmt_execute
 *               0x18   COM_STMT_SEND_LONG_DATA mysql_stmt_send_long_data
 *               0x19   COM_STMT_CLOSE      mysql_stmt_close
 *               0x1a   COM_STMT_RESET      mysql_stmt_reset
 *               0x1b   COM_SET_OPTION      mysql_set_server_option
 *               0x1c   COM_STMT_FETCH      mysql_stmt_fetch
 * 
 * arg:          The text of the command is just the way the user typed it, there is no processing
 *               by the client (except removal of the final ';').
 *               This field is not a null-terminated string; however,
 *               the size can be calculated from the packet size,
 *               and the MySQL client appends '\0' when receiving.
 *               
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Command_Packet_.28Overview.29
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-15 下午04:35:34
 */
public class CommandPacket extends MySQLPacket {

    public byte   command;
    public byte[] arg;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        arg = mm.readBytes();
    }

    public void write(OutputStream out) throws IOException {
        StreamUtil.writeUB3(out, getPacketLength());
        StreamUtil.write(out, packetId);
        StreamUtil.write(out, command);
        out.write(arg);
    }

    public void write(BackendConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, getPacketLength());
        buffer.put(packetId);
        buffer.put(command);
        buffer = c.writeToBuffer(arg, buffer);
        c.write(buffer);
    }

    protected int getPacketLength() {
        return 1 + arg.length;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Command Packet";
    }

}
