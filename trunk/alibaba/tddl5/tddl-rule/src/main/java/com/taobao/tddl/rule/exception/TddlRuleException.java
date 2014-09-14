package com.taobao.tddl.rule.exception;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * tddl rule exception
 * 
 * @author jianghang 2014-4-23 下午3:39:33
 * @since 5.1.0
 */
public class TddlRuleException extends TddlRuntimeException {

    private static final long serialVersionUID = 5802258037624186974L;

    public TddlRuleException(String... params){
        super(ErrorCode.ERR_ROUTE, params);
    }

    public TddlRuleException(Throwable cause, String... params){
        super(ErrorCode.ERR_ROUTE, cause, params);
    }

    public TddlRuleException(Throwable cause){
        super(ErrorCode.ERR_ROUTE, cause, new String[] { cause.getMessage() });
    }

}
