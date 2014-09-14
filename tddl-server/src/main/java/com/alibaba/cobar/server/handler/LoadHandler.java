package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.net.packet.BinaryPacket;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLLoadStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.util.StringUtil;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public final class LoadHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoadHandler.class);

    public static void handle(String sql, ServerConnection c) {
        DMLLoadStatement loadStmt = null;
        try {
            SQLStatement stmt = SQLParserDelegate.parse(sql);

            if (!(stmt instanceof DMLLoadStatement)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "not a load statement");
            }

            loadStmt = (DMLLoadStatement) stmt;
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ERR_PARSER.getCode(), e.getMessage());
        }

        try {
            String fileName = loadStmt.getFileName().getUnescapedString();
            BinaryPacket packet = new BinaryPacket();
            byte[] fileNameBytes = StringUtil.encode(fileName, c.getCharset());
            byte[] data = new byte[1 + fileNameBytes.length];
            data[0] = BinaryPacket.LOCAL_INFILE;
            System.arraycopy(fileNameBytes, 0, data, 1, fileNameBytes.length);
            packet.data = data;
            packet.packetId = c.getNewPacketId();
            c.setLoadFile(true);
            c.setLoadDataSql(sql);
            c.write(packet.write(c.allocate(), c));
            logger.info("start load data, sql is :" + sql);
        } catch (Exception ex) {
            logger.error("error in handle load file", ex);
            c.writeErrMessage(ErrorCode.ERR_EXECUTOR.getCode(), ex.getMessage());

        }

    }
}
