package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.parser.ServerParse;
import com.alibaba.cobar.server.parser.ServerParseSelect;
import com.alibaba.cobar.server.response.SelectDatabase;
import com.alibaba.cobar.server.response.SelectIdentity;
import com.alibaba.cobar.server.response.SelectLastInsertId;
import com.alibaba.cobar.server.response.SelectUser;
import com.alibaba.cobar.server.response.SelectVersion;
import com.alibaba.cobar.server.response.SelectVersionComment;
import com.alibaba.cobar.server.util.ParseUtil;

/**
 * @author xianmao.hexm
 */
public final class SelectHandler {

    public static void handle(String stmt, ServerConnection c, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c);
                break;
            case ServerParseSelect.USER:
                SelectUser.response(c);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c);
                break;
            case ServerParseSelect.LAST_INSERT_ID:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop: for (; offset < stmt.length(); ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case 'L':
                        case 'l':
                            break loop;
                    }
                }
                offset = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectLastInsertId.response(c, stmt, offset);
                break;
            case ServerParseSelect.IDENTITY:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop: for (; offset < stmt.length(); ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case '@':
                            break loop;
                    }
                }
                int indexOfAtAt = offset;
                offset += 2;
                offset = ServerParseSelect.indexAfterIdentity(stmt, offset);
                String orgName = stmt.substring(indexOfAtAt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectIdentity.response(c, stmt, offset, orgName);
                break;
            default:
                c.execute(stmt, ServerParse.SELECT);
        }
    }

}
