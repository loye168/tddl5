package com.alibaba.cobar.server.util;

import com.alibaba.cobar.parser.util.CharTypes;

/**
 * @author xianmao.hexm 2011-5-9 下午02:40:29
 */
public final class ParseUtil {

    public static boolean isEOF(char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';');
    }

    public static long getSQLId(String stmt) {
        int offset = stmt.indexOf('=');
        if (offset != -1 && stmt.length() > ++offset) {
            String id = stmt.substring(offset).trim();
            try {
                return Long.parseLong(id);
            } catch (NumberFormatException e) {
            }
        }
        return 0L;
    }

    /**
     * <code>'abc'</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>'</code>
     */
    private static String parseString(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                    case '0':
                        sb.append('\0');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'Z':
                        sb.append((char) 26);
                        break;
                    default:
                        sb.append(c);
                }
            } else if (c == '\'') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '\'') {
                    ++offset;
                    sb.append('\'');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>"abc"</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>"</code>
     */
    private static String parseString2(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                    case '0':
                        sb.append('\0');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'Z':
                        sb.append((char) 26);
                        break;
                    default:
                        sb.append(c);
                }
            } else if (c == '"') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '"') {
                    ++offset;
                    sb.append('"');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>AS `abc`</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>`</code>
     */
    private static String parseIdentifierEscape(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '`') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '`') {
                    ++offset;
                    sb.append('`');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * @param aliasIndex for <code>AS id</code>, index of 'i'
     */
    public static String parseAlias(String stmt, final int aliasIndex) {
        if (aliasIndex < 0 || aliasIndex >= stmt.length()) {
            return null;
        }
        switch (stmt.charAt(aliasIndex)) {
            case '\'':
                return parseString(stmt, aliasIndex);
            case '"':
                return parseString2(stmt, aliasIndex);
            case '`':
                return parseIdentifierEscape(stmt, aliasIndex);
            default:
                int offset = aliasIndex;
                for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset)
                    ;
                return stmt.substring(aliasIndex, offset);
        }
    }

    public static int comment(String stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        switch (stmt.charAt(n)) {
            case '/':
                if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1 && stmt.charAt(n) != '!') {
                    for (int i = n; i < len; ++i) {
                        if (stmt.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && stmt.charAt(m) == '/') return m;
                        }
                    }
                }
                break;
            case '#':
                for (int i = n + 1; i < len; ++i) {
                    if (stmt.charAt(i) == '\n') return i;
                }
                break;
        }
        return offset;
    }

    public static int move(String stmt, int offset, int length) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                case '#':
                    i = comment(stmt, i);
                    continue;
                default:
                    return i + length;
            }
        }
        return i;
    }

    public static boolean compare(String s, int offset, char[] keyword) {
        if (s.length() >= offset + keyword.length) {
            for (int i = 0; i < keyword.length; ++i, ++offset) {
                if (Character.toUpperCase(s.charAt(offset)) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
