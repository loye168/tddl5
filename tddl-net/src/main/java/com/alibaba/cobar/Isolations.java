package com.alibaba.cobar;

/**
 * 事务隔离级别定义
 * 
 * @author xianmao.hexm
 */
public enum Isolations {

    READ_UNCOMMITTED(1), READ_COMMITTED(2), REPEATED_READ(4), SERIALIZABLE(8);

    private int code;

    Isolations(int code){
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static Isolations valuesOf(String name) {
        for (Isolations iso : Isolations.values()) {
            if (iso.name().equals(name)) {
                return iso;
            }
        }

        throw new IllegalArgumentException("unknown isolations:" + name);
    }

}
