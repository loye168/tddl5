package com.taobao.tddl.common.exception;

import java.sql.SQLException;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * Tddl nestabled {@link Exception}
 * 
 * @author jianghang 2013-10-24 下午2:55:38
 * @since 5.0.0
 */
public class TddlException extends SQLException {

    private static final long serialVersionUID = 1540164086674285095L;

    public TddlException(ErrorCode errorCode, String... params){
        super(errorCode.getMessage(params), "ERROR", errorCode.getCode());
    }

    public TddlException(ErrorCode errorCode, Throwable cause, String... params){
        super(errorCode.getMessage(params), "ERROR", errorCode.getCode(), cause);
    }

    public TddlException(Throwable cause){
        super(cause.getMessage(), cause);
    }

    public String toString() {
        return getLocalizedMessage();
    }

    /**
     * 为解决嵌套Exception的异常输出问题，针对无message的递归获取cause的节点，保证拿到正确的cause异常 <br/>
     * 如果自己有设置过message，以当前异常的message为准
     */
    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        } else {
            Throwable ca = this;
            do {
                Throwable c = ExceptionUtils.getCause(ca);
                if (c != null) {
                    ca = c;
                } else {
                    break;
                }
            } while (ca.getMessage() == null);
            return ca.getMessage();
        }
    }
}
