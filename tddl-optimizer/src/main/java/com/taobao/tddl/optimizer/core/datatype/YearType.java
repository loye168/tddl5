package com.taobao.tddl.optimizer.core.datatype;

/**
 * 针对mysql中特殊的year type
 * 
 * @author jianghang 2014-5-23 下午5:22:28
 * @since 5.1.0
 */
public class YearType extends LongType {

    @Override
    public int getSqlType() {
        return YEAR_SQL_TYPE;
    }

}
