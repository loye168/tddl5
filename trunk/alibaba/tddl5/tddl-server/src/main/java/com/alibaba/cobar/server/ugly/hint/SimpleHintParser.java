package com.alibaba.cobar.server.ugly.hint;

import java.sql.SQLSyntaxErrorException;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public final class SimpleHintParser extends HintParser {

    @Override
    public void process(CobarHint hint, String hintName, String sql) throws SQLSyntaxErrorException {
        Object value = parsePrimary(hint, sql);
        if (value instanceof Long) {
            value = ((Long) value).intValue();
        }

        if ("table".equals(hintName)) {
            hint.setTable((String) value);
        } else if ("replica".equals(hintName)) {
            hint.setReplica((Integer) value);
        }
    }

}
