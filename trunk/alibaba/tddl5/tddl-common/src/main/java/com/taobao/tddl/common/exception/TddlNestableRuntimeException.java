package com.taobao.tddl.common.exception;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.exception.Nestable;
import org.apache.commons.lang.exception.NestableDelegate;

/**
 * tddl nestable exception, 为rethrow exception准备的
 * 
 * @author jianghang 2014-4-23 下午1:54:17
 * @since 5.1.0
 */
public class TddlNestableRuntimeException extends RuntimeException implements Nestable {

    private static final long  serialVersionUID = -363994535286446190L;

    /**
     * The helper instance which contains much of the code which we delegate to.
     */
    protected NestableDelegate delegate         = new NestableDelegate(this);

    /**
     * Holds the reference to the exception or error that caused this exception
     * to be thrown.
     */
    private Throwable          cause            = null;

    /**
     * Constructs a new <code>TddlNestableRuntimeException</code> without
     * specified detail message.
     */
    public TddlNestableRuntimeException(){
        super();
    }

    /**
     * Constructs a new <code>TddlNestableRuntimeException</code> with specified
     * detail message.
     * 
     * @param msg the error message
     */
    public TddlNestableRuntimeException(String msg){
        super(msg);
    }

    /**
     * Constructs a new <code>TddlNestableRuntimeException</code> with specified
     * nested <code>Throwable</code>.
     * 
     * @param cause the exception or error that caused this exception to be
     * thrown
     */
    public TddlNestableRuntimeException(Throwable cause){
        super();
        this.cause = cause;
    }

    /**
     * Constructs a new <code>TddlNestableRuntimeException</code> with specified
     * detail message and nested <code>Throwable</code>.
     * 
     * @param msg the error message
     * @param cause the exception or error that caused this exception to be
     * thrown
     */
    public TddlNestableRuntimeException(String msg, Throwable cause){
        super(msg);
        this.cause = cause;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Throwable getCause() {
        return cause;
    }

    /**
     * 为解决嵌套Exception的异常输出问题，针对无message的递归获取cause的节点，保证拿到正确的cause异常 <br/>
     * 如果自己有设置过message，以当前异常的message为准
     */
    @Override
    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        } else if (cause != null) {
            Throwable ca = cause;
            while (this.getClass().isInstance(ca) && ca.getMessage() == null) {
                Throwable c = ExceptionUtils.getCause(ca);
                if (c != null) {
                    ca = c;
                } else {
                    break;
                }
            }

            if (ca != null) {
                return ca.getMessage();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage(int index) {
        if (index == 0) {
            return super.getMessage();
        }
        return delegate.getMessage(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getMessages() {
        return delegate.getMessages();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Throwable getThrowable(int index) {
        return delegate.getThrowable(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getThrowableCount() {
        return delegate.getThrowableCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Throwable[] getThrowables() {
        return delegate.getThrowables();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int indexOfThrowable(Class type) {
        return delegate.indexOfThrowable(type, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int indexOfThrowable(Class type, int fromIndex) {
        return delegate.indexOfThrowable(type, fromIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printStackTrace() {
        delegate.printStackTrace();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printStackTrace(PrintStream out) {
        delegate.printStackTrace(out);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printStackTrace(PrintWriter out) {
        delegate.printStackTrace(out);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void printPartialStackTrace(PrintWriter out) {
        super.printStackTrace(out);
    }

    public String toString() {
        return super.getLocalizedMessage();
    }

}
