package com.taobao.tddl.rule.exception;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class RouteCompareDiffException extends TddlException {

    private static final long serialVersionUID = -1050306101643415508L;

    public RouteCompareDiffException(String... params){
        super(ErrorCode.ERR_ROUTE_COMPARE_DIFF, params);
    }

    public RouteCompareDiffException(Throwable cause, String... params){
        super(ErrorCode.ERR_ROUTE_COMPARE_DIFF, cause, params);
    }

}
