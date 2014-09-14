package com.alibaba.cobar.manager.parser;

import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.server.util.ParseUtil;
import com.alibaba.cobar.server.util.StringUtil;

/**
 * @author xianmao.hexm 2011-5-12 下午04:11:25
 */
public final class ManagerParseSwitch {

    public static final int OTHER      = -1;
    public static final int DATASOURCE = 1;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case '@':
                    return switch2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    public static Pair<String[], Integer> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(offset + 12).trim();
        int p1 = s.lastIndexOf(':');
        if (p1 == -1) {
            String[] src = StringUtil.split(s, ',', '$', '-', '[', ']');
            return new Pair<String[], Integer>(src, null);
        } else {
            String[] src = StringUtil.split(s, ':', true);
            String[] src1 = StringUtil.split(src[0], ',', '$', '-', '[', ']');
            return new Pair<String[], Integer>(src1, Integer.valueOf(src[1]));
        }
    }

    // DATASOURCE
    static int switch2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > offset + 10) {
                char c1 = stmt.charAt(++offset);
                char c2 = stmt.charAt(++offset);
                char c3 = stmt.charAt(++offset);
                char c4 = stmt.charAt(++offset);
                char c5 = stmt.charAt(++offset);
                char c6 = stmt.charAt(++offset);
                char c7 = stmt.charAt(++offset);
                char c8 = stmt.charAt(++offset);
                char c9 = stmt.charAt(++offset);
                char c10 = stmt.charAt(++offset);
                if ((c1 == 'D' || c1 == 'd') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't')
                    && (c4 == 'A' || c4 == 'a') && (c5 == 'S' || c5 == 's') && (c6 == 'O' || c6 == 'o')
                    && (c7 == 'U' || c7 == 'u') && (c8 == 'R' || c8 == 'r') && (c9 == 'C' || c9 == 'c')
                    && (c10 == 'E' || c10 == 'e')) {
                    if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                        return OTHER;
                    }
                    return DATASOURCE;
                }
            }
        }
        return OTHER;
    }

}
