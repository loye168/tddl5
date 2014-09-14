package com.taobao.tddl.common.model;

/**
 * 基于hint的thread local相关变量定义
 * 
 * @author jianghang 2013-10-23 下午5:38:42
 * @since 5.0.0
 */
public class ThreadLocalString {

    public static final String DB_SELECTOR           = "DB_SELECTOR";

    public static final String ROUTE_CONDITION       = "ROUTE_CONDITION";

    /**
     * 让GroupDataSource在指定序号的DATASOURCE上执行操作
     */
    public static final String DATASOURCE_INDEX      = "DATASOURCE_INDEX";

    /**
     * 如果指定了ds_index，如果对应库又不可用，应用希望让这个查询还是能够做，那么 让这个查询再走下权重(如果没有权重，也走下单库重试)
     */
    public static final String RETRY_IF_SET_DS_INDEX = "RETRY_IF_SET_DS_INDEX";

    /**
     * 指定是否使用并行执行
     */
    public static final String PARALLEL_EXECUTE      = "PARALLEL_EXECUTE";

    public static final String UNIT_VALID            = "UNIT_VALID";

    /**
     * txc事务机制context
     */
    public static final String TXC_CONTEXT           = "TXC_CONTEXT";

	/**
	 * 设置txc事务context时设置；事务结束时，检测到该标记，则TXC_CONTEXT，否则不清理
	 */
	public static final String TXC_CONTEXT_MANAGER   = "TXC_CONTEXT_MANAGER";
	public static final String TXC_MANAGER_NAME      = "TDDL"; 
}
