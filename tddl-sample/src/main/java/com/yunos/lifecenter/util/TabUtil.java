package com.yunos.lifecenter.util;

/**
 * Created by zhenggangji on 2/12/14.
 */
public class TabUtil {
    public static long beforeConvertValue(Object val) {
        if(val == null) return 0;
        return Math.abs(val.hashCode());
    }

    public static void main(String[] args) {
        System.out.println(beforeConvertValue(new String("5131778460132176")));
    }
}
