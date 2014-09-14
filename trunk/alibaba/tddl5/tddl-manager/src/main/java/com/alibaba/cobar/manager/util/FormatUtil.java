package com.alibaba.cobar.manager.util;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 格式化工具
 * 
 * @author xianmao.hexm
 * @version 2008-11-24 下午12:58:17
 */
public class FormatUtil {

    // 右对齐格式化字符串
    public static final int            ALIGN_RIGHT      = 0;

    // 左对齐格式化字符串
    public static final int            ALIGN_LEFT       = 1;

    private static final char          defaultSplitChar = ' ';

    private static final String[]      timeFormat       = new String[] { "d ", "h ", "m ", "s ", "ms" };

    private static final DecimalFormat numberFormat     = new DecimalFormat("###,###");

    private static final long          KB               = 1024L;
    private static final long          MB               = 1024L * 1024;
    private static final long          GB               = 1024L * 1024 * 1024;
    private static final long          TB               = 1024L * 1024 * 1024 * 1024;

    /**
     * 格式化后返回的字符串
     * 
     * @param s 需要格式化的原始字符串，默认按左对齐。
     * @param fillLength 填充长度
     * @return String
     */
    public static final String format(String s, int fillLength) {
        return format(s, fillLength, defaultSplitChar, ALIGN_LEFT);
    }

    /**
     * 格式化后返回的字符串
     * 
     * @param i 需要格式化的数字类型，默认按右对齐。
     * @param fillLength 填充长度
     * @return String
     */
    public static final String format(int i, int fillLength) {
        return format(Integer.toString(i), fillLength, defaultSplitChar, ALIGN_RIGHT);
    }

    /**
     * 格式化后返回的字符串
     * 
     * @param l 需要格式化的数字类型，默认按右对齐。
     * @param fillLength 填充长度
     * @return String
     */
    public static final String format(long l, int fillLength) {
        return format(Long.toString(l), fillLength, defaultSplitChar, ALIGN_RIGHT);
    }

    /**
     * @param s 需要格式化的原始字符串
     * @param fillLength 填充长度
     * @param fillChar 填充的字符
     * @param align 填充方式（左边填充还是右边填充）
     * @return String
     */
    public static final String format(String s, int fillLength, char fillChar, int align) {
        if (s == null) {
            s = "";
        } else {
            s = s.trim();
        }
        int charLen = fillLength - s.length();
        if (charLen > 0) {
            char[] fills = new char[charLen];
            for (int i = 0; i < charLen; i++) {
                fills[i] = fillChar;
            }
            StringBuilder str = new StringBuilder(s);
            switch (align) {
                case ALIGN_RIGHT:
                    str.insert(0, fills);
                    break;
                case ALIGN_LEFT:
                    str.append(fills);
                    break;
                default:
                    str.append(fills);
            }
            return str.toString();
        } else {
            return s;
        }
    }

    /**
     * 格式化时间输出
     * <p>
     * 1d 15h 4m 15s 987ms
     * </p>
     */
    public static final String formatTime(long millis, int precision) {
        long[] la = new long[5];
        la[0] = (millis / 86400000);// days
        la[1] = (millis / 3600000) % 24;// hours
        la[2] = (millis / 60000) % 60;// minutes
        la[3] = (millis / 1000) % 60;// seconds
        la[4] = (millis % 1000);// ms

        int index = 0;
        for (int i = 0; i < la.length; i++) {
            if (la[i] != 0) {
                index = i;
                break;
            }
        }

        StringBuilder buf = new StringBuilder();
        int validLength = la.length - index;
        for (int i = 0; (i < validLength && i < precision); i++) {
            buf.append(la[index]).append(timeFormat[index]);
            index++;
        }
        return buf.toString();
    }

    /**
     * 将时间戳转换为时间点
     * 
     * @param millis
     * @return // 1293690711302L -> 2010-12-30 14:31:51
     */
    public static final String fromMilliseconds2String(long millis) {
        Date date = new Date(millis);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(date);
    }

    /**
     * 返回完整串或者是“offer集...”
     * 
     * @param source 源串
     * @param length 最大显示长度
     * @return
     */
    public static final String shortString(String source, int length) {
        if (source.length() > length) {
            int endIndex = length - 1;
            String result = source.substring(0, endIndex) + "...";
            return result;
        }
        return source;
    }

    /**
     * 将jdbc版本号转换为cobar的版本号
     * 
     * @param src
     * @return 5.1.00-cobar-1.1.0 -> cobar-1.1.0
     */
    public static final String formatVersion(String src) {
        if (src != null) {
            int index = src.indexOf('-');
            if (index != -1) return src.substring(index + 1);
        }
        return src;

    }

    /**
     * 内存大小、网络总流量、磁盘存储等格式化带单位输出
     * 
     * @param store
     * @return 2048 -> 2KB , 123 -> 123B
     */
    public static String formatStore(long store) {
        if (store > TB) {
            double mem = divider(store, TB, 2);
            return new StringBuilder(String.valueOf(mem)).append(" ").append("TB").toString();
        } else if (store > GB) {
            double mem = divider(store, GB, 2);
            return new StringBuilder(String.valueOf(mem)).append(" ").append("GB").toString();
        } else if (store > MB) {
            double mem = divider(store, MB, 2);
            return new StringBuilder(String.valueOf(mem)).append(" ").append("MB").toString();
        } else if (store > KB) {
            double mem = divider(store, KB, 2);
            return new StringBuilder(String.valueOf(mem)).append(" ").append("KB").toString();
        } else {
            return new StringBuilder(String.valueOf(store)).append(" ").append("B").toString();
        }
    }

    /**
     * 四舍五入除法
     * 
     * @param d1 被除数
     * @param d2 除数
     * @param scale 结果精度(小数位数)
     * @return d1/d2, if(d2==0),return 0;
     */

    public static double divider(double d1, double d2, int scale) {
        if (d2 == 0) {
            return 0;
        }
        BigDecimal bd1 = new BigDecimal(Double.toString(d1));
        BigDecimal bd2 = new BigDecimal(Double.toString(d2));
        return bd1.divide(bd2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 网络流量格式化带单位输出
     * 
     * @param network
     * @return 2048 -> 2KB/s
     */
    public static String formatNetwork(long network) {
        StringBuilder sb = new StringBuilder(formatStore(network));
        return sb.append("/s").toString();
    }

    /**
     * 数据格式化为千位分隔符
     * 
     * @param number
     * @return 123456 -> 123,456
     */
    public static String formatNumber(long number) {
        return numberFormat.format(number);
    }

}
