package com.taobao.tddl.common.model;

/**
 * @author linxuan
 */
public enum SqlType {

    /** */
    SELECT(0),
    /** */
    INSERT(1),
    /** */
    UPDATE(2),
    /** */
    DELETE(3),
    /** */
    SELECT_FOR_UPDATE(4),
    /** */
    REPLACE(5),
    /** */
    TRUNCATE(6),
    /** */
    CREATE(7),
    /** */
    DROP(8),
    /** */
    LOAD(9),
    /** */
    MERGE(10),
    /** */
    SHOW(11),
    /** */
    ALTER(12),
    /** */
    RENAME(13),
    /** */
    DUMP(14),
    /** */
    DEBUG(15),
    /** */
    EXPLAIN(16),
    /** 存储过程 */
    PROCEDURE(17),
    /** */
    DESC(18),
    /** 获取上一个insert id */
    SELECT_LAST_INSERT_ID(19),
    /** */
    SELECT_WITHOUT_TABLE(20),
    /** */
    CREATE_SEQUENCE(21), SHOW_SEQUENCES(22), GET_SEQUENCE(23),
    /** */
    TDDL_SHOW(24),
    /** */
    SET(25), RELOAD(26);

    private int i;

    private SqlType(int i){
        this.i = i;
    }

    public int value() {
        return this.i;
    }

    public static SqlType valueOf(int i) {
        for (SqlType t : values()) {
            if (t.value() == i) {
                return t;
            }
        }
        throw new IllegalArgumentException("Invalid SqlType:" + i);
    }
}
