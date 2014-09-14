package com.taobao.tddl.common.exception;

import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * Tddl nestabled {@link RuntimeException}
 * 
 * @author jianghang 2013-10-24 下午2:55:22
 * @since 5.0.0
 */
public class TddlRuntimeException extends TddlNestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    private int               vendorCode       = 0;

    public TddlRuntimeException(ErrorCode errorCode, String... params){
        super(errorCode.getMessage(params));
        this.vendorCode = errorCode.getCode();
    }

    public TddlRuntimeException(ErrorCode errorCode, Throwable cause, String... params){
        super(errorCode.getMessage(params), cause);
        this.vendorCode = errorCode.getCode();
    }

    public String toString() {
        return super.getLocalizedMessage();
    }

    @Override
    public String getMessage() {
        if (vendorCode > 0) {
            return super.getMessage();
        } else {
            return super.getMessage();
        }
    }
}
