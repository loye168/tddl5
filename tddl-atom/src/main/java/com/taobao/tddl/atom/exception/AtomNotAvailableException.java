package com.taobao.tddl.atom.exception;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * Atom层通过ExceptionSorter检测到数据源不可用时抛出， 或者数据库不可用，同时没有trylock到重试机会时也抛出 便于group层重试
 * 
 * @author linxuan
 */
public class AtomNotAvailableException extends TddlException {

    private static final long serialVersionUID = 1L;

    public AtomNotAvailableException(String... params){
        super(ErrorCode.ERR_ATOM_NOT_AVALILABLE, params);
    }

    public AtomNotAvailableException(Throwable cause, String... params){
        super(ErrorCode.ERR_ATOM_NOT_AVALILABLE, cause, params);
    }

    public AtomNotAvailableException(Throwable cause){
        super(ErrorCode.ERR_ATOM_NOT_AVALILABLE, cause);
    }

}
