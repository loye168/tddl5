package com.taobao.tddl.group.exception;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * 当一组的数据库都试过，都不可用了，并且没有更多的数据源了，抛出该错误
 * 
 * @author linxuan
 */
public class GroupNotAvaliableException extends TddlException {

    private static final long serialVersionUID = -1473436753940538617L;

    public GroupNotAvaliableException(String... params){
        super(ErrorCode.ERR_GROUP_NOT_AVALILABLE, params);
    }

    public GroupNotAvaliableException(Throwable cause, String... params){
        super(ErrorCode.ERR_GROUP_NOT_AVALILABLE, cause, params);
    }

    public GroupNotAvaliableException(Throwable cause){
        super(ErrorCode.ERR_GROUP_NOT_AVALILABLE, cause);
    }

}
