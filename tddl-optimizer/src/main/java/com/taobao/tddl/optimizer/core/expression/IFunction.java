package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

/**
 * 代表一个函数列，比如max(id)
 * 
 * @author jianghang 2013-11-8 下午1:29:03
 * @since 5.0.0
 */
public interface IFunction<RT extends IFunction> extends ISelectable<RT> {

    public static interface BuiltInFunction {

        final static String EQUAL           = "=";
        final static String ADD             = "+";
        final static String SUB             = "-";
        final static String MULTIPLY        = "*";
        final static String DIVISION        = "/";
        final static String MOD             = "%";
        final static String BITAND          = "&";
        final static String BITOR           = "|";
        final static String BITXOR          = "^";
        final static String BITLSHIFT       = "<<";
        final static String BITRSHIFT       = ">>";
        final static String BITNOT          = "~";
        final static String MINUS           = "MINUS";
        final static String ROW             = "ROW";
        final static String AVG             = "AVG";
        final static String SUM             = "SUM";
        final static String COUNT           = "COUNT";
        final static String MAX             = "MAX";
        final static String MIN             = "MIN";
        final static String INTERVAL        = "INTERVAL_PRIMARY";
        final static String GET_FORMAT      = "GET_FORMAT";
        final static String EXTRACT         = "EXTRACT";
        final static String TIMESTAMPADD    = "TIMESTAMPADD";
        final static String TIMESTAMPDIFF   = "TIMESTAMPDIFF";
        final static String CONVERT         = "CONVERT";
        final static String CASE_WHEN       = "CASE_WHEN";
        final static String CAST            = "CAST";
        final static String CHAR            = "CHAR";
        // subquery
        final static String SUBQUERY_SCALAR = "SUBQUERY_SCALAR";
        // 特殊处理in比较，subquery可返回多条记录，比如id in (subquery)
        final static String SUBQUERY_LIST   = "SUBQUERY_LIST";
    }

    public enum FunctionType {
        /** 函数的操作面向一系列的值，并返回一个单一的值，可以理解为聚合函数 */
        Aggregate,
        /** 函数的操作面向某个单一的值，并返回基于输入值的一个单一的值，可以理解为转换函数 */
        Scalar;
    }

    public String getFunctionName();

    public FunctionType getFunctionType();

    public IFunction setFunctionName(String funcName);

    public List getArgs();

    public RT setArgs(List objs);

    public boolean isNeedDistinctArg();

    public RT setNeedDistinctArg(boolean b);

    /**
     * 获取执行函数实现
     */
    public IExtraFunction getExtraFunction();

    /**
     * 设置执行函数
     */
    public RT setExtraFunction(IExtraFunction function);
}
