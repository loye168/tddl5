package com.taobao.tddl.common.jdbc;

/**
 * 参数上下文
 * 
 * <pre>
 * 包含两个对象：
 * 1. parameterMethod枚举类型，表明对应的set方法类型，比如setString
 * 2. args数组，代表传递给setXXX的参数内容
 * </pre>
 * 
 * @author shenxun
 */
public class ParameterContext {

    private ParameterMethod parameterMethod;
    /**
     * args[0]: parameterIndex args[1]: 参数值 args[2]: length
     * 适用于：setAsciiStream、setBinaryStream、setCharacterStream、setUnicodeStream
     * 。。。
     */
    private Object[]        args;

    public ParameterContext(){
    }

    public ParameterContext(ParameterMethod parameterMethod, Object[] args){
        this.parameterMethod = parameterMethod;
        this.args = args;
    }

    public ParameterMethod getParameterMethod() {
        return parameterMethod;
    }

    public void setParameterMethod(ParameterMethod parameterMethod) {
        this.parameterMethod = parameterMethod;
    }

    public Object[] getArgs() {
        return args;
    }

    public Object getValue() {
        return args[1];
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public void setValue(Object value) {
        this.args[1] = value;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(parameterMethod).append("(");
        for (int i = 1; i < args.length; ++i) {
            buffer.append(args[i]);
            if (i != args.length - 1) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        return buffer.toString();
    }
}
