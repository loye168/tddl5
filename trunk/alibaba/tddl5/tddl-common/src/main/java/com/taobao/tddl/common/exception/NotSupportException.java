package com.taobao.tddl.common.exception;

import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * 未支持
 * 
 * @author jianghang 2014-4-23 上午11:32:33
 * @since 5.1.0
 */
public class NotSupportException extends TddlRuntimeException {

    private static final long serialVersionUID = 3333002727706650503L;

    public NotSupportException(){
        super(ErrorCode.ERR_NOT_SUPPORT, "");
    }

    public NotSupportException(String... params){
        super(ErrorCode.ERR_NOT_SUPPORT, params);
    }
    
}
