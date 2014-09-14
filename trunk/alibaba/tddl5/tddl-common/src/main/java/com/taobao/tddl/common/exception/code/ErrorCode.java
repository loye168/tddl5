package com.taobao.tddl.common.exception.code;

/**
 * 返回值定义的code对象定义
 * 
 * @author jianghang 2014-3-20 下午6:02:00
 * @since 5.1.0
 */
public enum ErrorCode {

    // ============ config 从4000下标开始==============
    //
    ERR_CONFIG(ErrorType.Config, 4000),
    //
    ERR_CONFIG_MISS_GROUPKEY(ErrorType.Config, 4001),
    //
    ERR_CONFIG_MISS_RULE(ErrorType.Config, 4002),
    //
    ERR_CONFIG_MISS_TOPOLOGY(ErrorType.Config, 4003),
    //
    ERR_CONFIG_MISS_PASSWD(ErrorType.Config, 4004),
    //
    ERR_CONFIG_MISS_ATOM_CONFIG(ErrorType.Config, 4005),

    //
    ERR_CONFIG_MISS_DBGROUPS(ErrorType.Config, 4006),

    // ============= atom 从4100下标开始================
    /** */
    ERR_ATOM_NOT_AVALILABLE(ErrorType.Atom, 4100),

    // ============= group 从4020下标开始================
    /** */
    ERR_GROUP_NOT_AVALILABLE(ErrorType.Group, 4200),
    /** */
    ERR_SQLFORBID(ErrorType.Group, 4201),

    // ============= route 从4030下标开始================
    /** */
    ERR_ROUTE(ErrorType.Route, 4300),
    /** */
    ERR_ROUTE_COMPARE_DIFF(ErrorType.Route, 4301),

    // ============= sequence 从4400下标开始================
    /** */
    ERR_SEQUENCE(ErrorType.Sequence, 4400),
    //
    ERR_MISS_SEQUENCE(ErrorType.Sequence, 4401),

    // 没有设default db，读不到sequence表
    ERR_MISS_SEQUENCE_DEFAULT_DB(ErrorType.Sequence, 4402),

    // 缺少sequence表
    ERR_MISS_SEQUENCE_TABLE_ON_DEFAULT_DB(ErrorType.Sequence, 4403),
    // sequence表结构不对
    ERR_SEQUENCE_TABLE_META(ErrorType.Sequence, 4404),

    ERR_INIT_SEQUENCE_FROM_DB(ErrorType.Sequence, 4405),

    ERR_LOAD_SEQUENCE_FROM_DB(ErrorType.Sequence, 4406),

    ERR_OTHER_WHEN_BUILD_SEQUENCE(ErrorType.Sequence, 4407),

    ERR_SEQUENCE_NEXT_VALUE(ErrorType.Sequence, 4408),

    // ============= optimizer 从4500下标开始================
    /** */
    ERR_PARSER(ErrorType.Parser, 4500),
    /** */
    ERR_OPTIMIZER(ErrorType.Optimizer, 4501),
    /** group/order存在函数列 */
    ERR_OPTIMIZER_MISS_ORDER_FUNCTION_IN_SELECT(ErrorType.Optimizer, 4502),
    /** 缺少join filter */
    ERR_OPTIMIZER_MISS_JOIN_FILTER(ErrorType.Optimizer, 4503),
    // ============= executor 从4600下标开始================
    //
    ERR_FUNCTION(ErrorType.Executor, 4600),
    //
    ERR_EXECUTOR(ErrorType.Executor, 4601),
    //
    ERR_CONVERTOR(ErrorType.Executor, 4602),

    ERR_ACCROSS_DB_TRANSACTION(ErrorType.Executor, 4603),

    ERR_CONCURRENT_TRANSACTION(ErrorType.Executor, 4604),

    ERR_ROLLBACK_AUTOCOMMIT_TRANSACTION(ErrorType.Executor, 4605),

    // ============= server 从4700下标开始================
    ERR_SERVER(ErrorType.Server, 4700),

    // ============= executor 从4900下标开始================
    //
    ERR_ASSERT_NULL(ErrorType.Other, 4995),
    //
    ERR_ASSERT_TRUE(ErrorType.Other, 4996),
    //
    ERR_ASSERT_FAIL(ErrorType.Other, 4997),
    /** */
    ERR_NOT_SUPPORT(ErrorType.Other, 4998),
    /** 其他未知 */
    ERR_OTHER(ErrorType.Other, 4999);

    ErrorCode(ErrorType type, int code){
        this.code = code;
        this.type = type;
    }

    private int       code;
    private ErrorType type;

    public int getCode() {
        return code;
    }

    public String getType() {
        return type.name();
    }

    public String getMessage(String... params) {
        return ResourceBundleUtil.getInstance().getMessage(this.name(), this.getCode(), this.getType(), params);
    }
}

enum ErrorType {
    Config, Atom, Group, Route, Sequence, Parser, Optimizer, Executor, Server, Other;
}
