package com.taobao.tddl.optimizer.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

import com.taobao.tddl.optimizer.core.ast.ASTNode;

/**
 * 空结果的过滤条件异常，比如 0 = 1的条件
 * 
 * @author jianghang 2013-11-13 下午4:05:45
 * @since 5.0.0
 */
public class EmptyResultFilterException extends NestableRuntimeException {

    private static final long serialVersionUID = -7525463650321091760L;
    private ASTNode           astNode;

    public ASTNode getAstNode() {
        return astNode;
    }

    public void setAstNode(ASTNode astNode) {
        this.astNode = astNode;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}
