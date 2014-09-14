package com.taobao.tddl.optimizer.core.expression;


/**
 * sequence.nextval
 * 
 * @author jianghang 2014-4-28 下午4:10:00
 * @since 5.1.0
 */
public interface ISequenceVal extends IBindVal, Comparable {

    public static final String SEQ_NEXTVAL = "NEXTVAL";

    public void setOriginIndex(int index);

}
