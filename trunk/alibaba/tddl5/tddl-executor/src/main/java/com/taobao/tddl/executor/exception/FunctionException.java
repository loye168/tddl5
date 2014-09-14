package com.taobao.tddl.executor.exception;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class FunctionException extends TddlRuntimeException {

    private static final long serialVersionUID = 7119345874409878404L;

    public FunctionException(String... params){
        super(ErrorCode.ERR_FUNCTION, params);
    }

    public FunctionException(Throwable cause, String... params){
        super(ErrorCode.ERR_FUNCTION, cause, params);
    }

    public FunctionException(Throwable cause){
        super(ErrorCode.ERR_FUNCTION, cause, new String[] { cause.getMessage() });
    }

}
