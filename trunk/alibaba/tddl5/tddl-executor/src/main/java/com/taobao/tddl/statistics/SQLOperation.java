package com.taobao.tddl.statistics;

public interface SQLOperation {

    String getOperationString();

    String getOperationType();

    String getSqlOrResult();

    String getParamsStr();

}
