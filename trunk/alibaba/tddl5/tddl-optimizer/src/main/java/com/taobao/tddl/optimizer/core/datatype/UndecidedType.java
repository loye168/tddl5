package com.taobao.tddl.optimizer.core.datatype;

/**
 * 未决类型
 * 
 * @author jianghang 2014-5-23 下午8:27:59
 * @since 5.1.0
 */
public class UndecidedType extends StringType {

    @Override
    public int getSqlType() {
        return UNDECIDED_SQL_TYPE;
    }
}
