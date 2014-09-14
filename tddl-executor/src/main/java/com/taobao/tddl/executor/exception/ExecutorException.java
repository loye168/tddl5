package com.taobao.tddl.executor.exception;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class ExecutorException extends TddlRuntimeException {

    private static final long serialVersionUID = 7119345874409878404L;

    public ExecutorException(String... params){
        super(ErrorCode.ERR_EXECUTOR, params);
    }

    public ExecutorException(Throwable cause, String... params){
        super(ErrorCode.ERR_EXECUTOR, cause, params);
    }

    public ExecutorException(Throwable cause){
        super(ErrorCode.ERR_EXECUTOR, cause, new String[] { cause.getMessage() });
    }

}
