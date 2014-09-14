package com.alibaba.cobar.manager.handler;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.manager.parser.ManagerParseClear;

/**
 * @author xianmao.hexm 2012-4-16
 */
public class ClearHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseClear.parse(stmt, offset);
        switch (rs & 0xff) {

            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}
