package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * 标准的Bit类型实现
 * 
 * @author jianghang 2014-1-21 下午5:37:38
 * @since 5.0.0
 */
public class BigBitType extends BigDecimalType {

    public BigBitType(){
        super();
        convertor = ConvertorHelper.bitBytesToBigDecimal;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.DECIMAL;
    }

}
