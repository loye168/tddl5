package com.taobao.tddl.statistics;

import com.taobao.tddl.common.jdbc.Parameters;

/**
 * @author mengshi.sunmengshi 2014年6月24日 下午3:12:09
 * @since 5.1.0
 */
public abstract class AbstractSQLOperation implements SQLOperation {

    private Parameters params;

    public void setParams(Parameters params) {
        this.params = params;
    }

    @Override
    public String getParamsStr() {

        if (params == null) {
            return null;
        }

        if (params.getBatchSize() == 0) {
            return null;
        }
        return params.toString();
    }

}
