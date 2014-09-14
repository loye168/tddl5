package com.alibaba.cobar.manager.handler;

import static com.alibaba.cobar.manager.parser.ManagerParseSelect.SESSION_AUTO_INCREMENT;
import static com.alibaba.cobar.manager.parser.ManagerParseSelect.SESSION_TX_ISOLATION;
import static com.alibaba.cobar.manager.parser.ManagerParseSelect.VERSION_COMMENT;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.manager.parser.ManagerParseSelect;
import com.alibaba.cobar.manager.response.SelectSessionAutoIncrement;
import com.alibaba.cobar.manager.response.SelectSessionTxIsolation;
import com.alibaba.cobar.manager.response.SelectVersionComment;

/**
 * @author xianmao.hexm
 */
public final class SelectHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSelect.parse(stmt, offset)) {
            case VERSION_COMMENT:
                SelectVersionComment.execute(c);
                break;
            case SESSION_AUTO_INCREMENT:
                SelectSessionAutoIncrement.execute(c);
                break;
            case SESSION_TX_ISOLATION:
                SelectSessionTxIsolation.execute(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
