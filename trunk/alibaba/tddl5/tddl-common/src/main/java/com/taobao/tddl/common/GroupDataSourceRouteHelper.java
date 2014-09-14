package com.taobao.tddl.common;

import com.taobao.tddl.common.client.util.ThreadLocalMap;

/**
 * 提供给单独使用GroupDataSource的用户指定数据源以及相关执行信息
 * 
 * @author junyu
 */
public class GroupDataSourceRouteHelper {

    /**
     * 让GroupDataSource在指定序号的DATASOURCE上执行操作
     */
    public static final String DATASOURCE_INDEX      = "DATASOURCE_INDEX";

    /**
     * 如果指定了ds_index，如果对应库又不可用，应用希望让这个查询还是能够做，那么 让这个查询再走下权重(如果没有权重，也走下单库重试)
     */
    public static final String RETRY_IF_SET_DS_INDEX = "RETRY_IF_SET_DS_INDEX";

    /**
     * 从一组数据源中选择一个指定序号上的数据源执行SQL。 如：groupKey=ExampleGroup 对应的content为
     * db1:rw,db2:r,db3:r
     * 
     * <pre>
     * RouteHelper.executeByGroupDataSourceIndex(2);
     * jdbcTemplate.queryForList(sql);
     * </pre>
     * 
     * 最终查询肯定会在第三个数据源上执行（db3） 注意，指定db的读写特性需要满足要求，如不可在 指定只读数据源上进行写操作，否则抛错。
     * 
     * @author junyu
     * @param dataSourceIndex 在指定Group中，所需要执行的db序号
     */
    public static void executeByGroupDataSourceIndex(int dataSourceIndex) {
        ThreadLocalMap.put(DATASOURCE_INDEX, dataSourceIndex);
    }

    public static void executeByGroupDataSourceIndex(int dataSourceIndex, boolean failRetry) {
        ThreadLocalMap.put(DATASOURCE_INDEX, dataSourceIndex);
        ThreadLocalMap.put(RETRY_IF_SET_DS_INDEX, failRetry);
    }

    /**
     * 为了保证一个线程执行多个操作不造成混乱(例如事务中作多个操作)，
     * 请对每个业务方法做try-finally，并在finally中调用该方法清除index: try{
     * GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
     * xxxDao.bizOperationxxx(); }finally{
     * GroupDataSourceRouteHelper.removeGroupDataSourceIndex(); }
     */
    public static void removeGroupDataSourceIndex() {
        ThreadLocalMap.remove(DATASOURCE_INDEX);
    }
}
