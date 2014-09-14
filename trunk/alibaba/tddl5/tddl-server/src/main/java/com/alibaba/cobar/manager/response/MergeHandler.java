package com.alibaba.cobar.manager.response;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.config.SchemaConfig;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.MysqlResultSetPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.net.util.CharsetUtil;
import com.alibaba.cobar.server.executor.utils.MysqlDefs;
import com.alibaba.cobar.server.util.StringUtil;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class MergeHandler {

    public static void execute(ManagerConnection c, String sql) {
        Map<String, SchemaConfig> schemas = CobarServer.getInstance().getConfig().getSchemas();
        MysqlResultSetPacket packet = new MysqlResultSetPacket();

        try {
            for (SchemaConfig schema : schemas.values()) {
                Connection conn = null;
                try {
                    TDataSource ds;
                    ResultSet rs;
                    try {
                        ds = schema.getDataSource();
                        ds.init();
                        conn = ds.getConnection();
                        rs = conn.prepareStatement(sql).executeQuery();
                    } catch (Throwable ex) {
                        continue;
                    }
                    resultSetToPacket(packet, rs, c.getCharset());

                } finally {
                    if (conn != null) {
                        conn.close();
                    }
                }
            }

            c.write(packet.write(c.allocate(), c));
        } catch (Exception ex) {
            c.handleError(ErrorCode.ERR_HANDLE_DATA, ex);
        }

    }

    public static void resultSetToPacket(MysqlResultSetPacket packet, ResultSet rs, String charset)
                                                                                                   throws SQLException,
                                                                                                   UnsupportedEncodingException {
        ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        if (packet.resulthead == null) {
            packet.resulthead = new ResultSetHeaderPacket();
            packet.resulthead.fieldCount = colunmCount;

            int charsetIndex = CharsetUtil.getIndex(charset);
            if (colunmCount > 0) {
                if (packet.fieldPackets == null) {
                    packet.fieldPackets = new FieldPacket[colunmCount];
                    for (int i = 0; i < colunmCount; i++) {
                        int j = i + 1;

                        packet.fieldPackets[i] = new FieldPacket();
                        packet.fieldPackets[i].catalog = StringUtil.encode("def", charset);
                        packet.fieldPackets[i].orgName = StringUtil.encode(metaData.getColumnName(j), charset);
                        packet.fieldPackets[i].name = StringUtil.encode(metaData.getColumnLabel(j), charset);
                        packet.fieldPackets[i].orgTable = StringUtil.encode(metaData.getTableName(j), charset);
                        packet.fieldPackets[i].table = StringUtil.encode(metaData.getTableName(j), charset);
                        packet.fieldPackets[i].db = StringUtil.encode(metaData.getSchemaName(j), charset);
                        packet.fieldPackets[i].length = 20;
                        packet.fieldPackets[i].flags = toFlag(metaData, j);
                        packet.fieldPackets[i].decimals = (byte) metaData.getScale(j);
                        packet.fieldPackets[i].charsetIndex = charsetIndex;

                        packet.fieldPackets[i].type = (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(metaData.getColumnType(j),
                            packet.fieldPackets[i].decimals)) & 0xff);
                    }
                }
            }
        }

        while (rs.next()) {
            RowDataPacket row = new RowDataPacket(colunmCount);
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                row.fieldValues.add(rs.getBytes(j));

            }
            packet.addRowDataPacket(row);
        }
    }

    public static int toFlag(java.sql.ResultSetMetaData metaData, int column) throws SQLException {
        int flags = 0;
        if (metaData.isNullable(column) == 1) {
            flags |= 0001;
        }

        if (metaData.isSigned(column)) {
            flags |= 0020;
        }

        if (metaData.isAutoIncrement(column)) {
            flags |= 0200;
        }

        return flags;
    }

}
