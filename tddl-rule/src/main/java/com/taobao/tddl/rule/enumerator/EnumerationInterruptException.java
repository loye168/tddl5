package com.taobao.tddl.rule.enumerator;

import com.taobao.tddl.common.model.sqljep.Comparative;

public class EnumerationInterruptException extends RuntimeException {

    private static final long     serialVersionUID = 1L;
    private transient Comparative comparative;

    public EnumerationInterruptException(Comparative comparative){
        super(comparative.toString());
        this.comparative = comparative;
    }

    public Comparative getComparative() {
        return comparative;
    }

    public void setComparative(Comparative comparative) {
        this.comparative = comparative;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}
