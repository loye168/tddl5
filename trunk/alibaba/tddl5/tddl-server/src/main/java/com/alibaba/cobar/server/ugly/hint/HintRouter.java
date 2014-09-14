package com.alibaba.cobar.server.ugly.hint;

import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.AND;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.DBID;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.DBINDEX;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.EXPR;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.PARAMS;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.RELATION;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.TDDL_HINT_UGLY_PREFIX;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.TDDL_HINT_UGLY_PREFIX_COMMENT;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.TYPE;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.TYPE_CONDITION;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.TYPE_DIRECT;
import static com.taobao.tddl.optimizer.parse.hint.SimpleHintParser.VTAB;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 兼容下以前drds暴露的hint格式，转换成tddl hint
 * 
 * @author jianghang 2014-5-16 下午8:25:52
 * @since 5.1.0
 */
public class HintRouter {

    public static String convertHint(String sql) throws SQLSyntaxErrorException {
        int index = indexOfPrefix(sql);
        if (index >= 0) {
            CobarHint hint = CobarHint.parserCobarHint(sql, index);
            return convertHint(hint) + hint.getOutputSql();
        } else {
            return convertUglyTddlHint(sql);
        }
    }

    private static String convertUglyTddlHint(String sql) {
        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }
            break;
        }

        return StringUtils.replace(sql, TDDL_HINT_UGLY_PREFIX, TDDL_HINT_UGLY_PREFIX_COMMENT);
    }

    public static int indexOfPrefix(String sql) {
        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }
            break;
        }

        if (sql.startsWith(CobarHint.COBAR_HINT_PREFIX, i)) {
            return i + CobarHint.COBAR_HINT_PREFIX.length();
        } else if (sql.startsWith(CobarHint.COBAR_HINT_UGLY_PREFIX, i)) {
            return i + CobarHint.COBAR_HINT_UGLY_PREFIX.length();
        } else {
            return -1;
        }
    }

    private static String convertHint(CobarHint hint) {
        StringBuilder builder = new StringBuilder();
        builder.append("/*+TDDL(");
        JSONObject data = new JSONObject();
        data.put(VTAB, hint.getTable());
        if (hint.getDataNodes() != null) {
            data.put(TYPE, TYPE_DIRECT);
            data.put(DBINDEX, true);
            List<Pair<Integer, Integer>> pairs = hint.getDataNodes();
            Object[] dbIds = new Object[pairs.size()];
            int i = 0;
            for (Pair<Integer, Integer> pair : pairs) {
                dbIds[i++] = pair.getKey();
            }
            data.put(DBID, StringUtils.join(dbIds, ","));
        } else if (hint.getPartitionOperand() != null) {
            data.put(TYPE, TYPE_CONDITION);
            JSONArray params = new JSONArray();
            Pair<String[], Object[][]> pair = hint.getPartitionOperand();
            String[] keys = pair.getKey();
            Object[][] values = pair.getValue();
            for (int i = 0; i < keys.length; i++) {
                JSONObject param = new JSONObject();
                param.put(RELATION, AND);
                JSONArray exprs = new JSONArray();
                param.put(EXPR, exprs);
                for (int j = 0; j < values.length; j++) {
                    exprs.add(buildComparativeCondition(keys[i], values[j][i]));
                }
                params.add(param);
            }
            data.put(PARAMS, params);
        }

        builder.append(data.toJSONString());
        builder.append(")*/");
        return builder.toString();
    }

    private static String buildComparativeCondition(String key, Object value) {
        String type = "s";
        if (value instanceof Number) {
            type = "l";
        }
        return key + "=" + ObjectUtils.toString(value) + ":" + type;
    }
}
