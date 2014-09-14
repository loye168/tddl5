package com.taobao.tddl.rule.enumerator.handler;

import java.util.Set;

import com.taobao.tddl.common.model.sqljep.Comparative;

/**
 * 如果不能进行枚举，那么就是用默认的枚举器 默认枚举器只支持comparativeOr条件，以及等于的关系。不支持大于小于等一系列关系。
 * 
 * @author shenxun
 */
public class DefaultEnumerator implements CloseIntervalFieldsEnumeratorHandler {

    public void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                      Integer cumulativeTimes, Comparable<?> atomIncrValue) {
        throw new IllegalArgumentException("default enumerator not support traversal");

    }

    public void processAllPassableFields(Comparative source, Set<Object> retValue, Integer cumulativeTimes,
                                         Comparable<?> atomIncrValue) {
        throw new IllegalStateException("default enumerator not support traversal, not support > < >= <=");
    }
}
