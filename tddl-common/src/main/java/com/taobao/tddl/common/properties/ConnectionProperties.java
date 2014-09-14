package com.taobao.tddl.common.properties;

/**
 * 用于执行的ExtraCmd
 * 
 * @author Dreamond
 */
public class ConnectionProperties {

    /**
     * 是否开启优化器cache
     */
    public static final String OPTIMIZER_CACHE                             = "OPTIMIZER_CACHE";

    /**
     * 是否选取索引，默认为true
     */
    public final static String CHOOSE_INDEX                                = "CHOOSE_INDEX";

    /**
     * 是否选择最优join策略, 默认为false
     */
    public final static String CHOOSE_JOIN                                 = "CHOOSE_JOIN";

    /**
     * 是否将or条件转化为index merge，默认为false
     */
    public final static String CHOOSE_INDEX_MERGE                          = "CHOOSE_INDEX_MERGE";

    /**
     * 智能优化join merge join，默认为true
     */
    public final static String JOIN_MERGE_JOIN_JUDGE_BY_RULE               = "JOIN_MERGE_JOIN_JUDGE_BY_RULE";

    /**
     * 是否强制优化成join merge join，默认为false
     */
    public final static String JOIN_MERGE_JOIN                             = "JOIN_MERGE_JOIN";

    /**
     * 是否展开Merge Join Merge，默认为false
     */
    public final static String MERGE_EXPAND                                = "MERGE_EXPAND";

    /**
     * 是否强制设置Merge并行执行，默认为空值,也就是会选择自适应模式
     */
    public final static String MERGE_CONCURRENT                            = "MERGE_CONCURRENT";

    /**
     * 是否展开in，把in转换成or
     */
    public final static String EXPAND_IN                                   = "EXPAND_IN";

    /**
     * 是否展开 a and (b or c)
     */
    public final static String EXPAND_OR                                   = "EXPAND_OR";

    /**
     * 表的meta超时时间，单位毫秒，默认5分钟
     */
    public static final String TABLE_META_CACHE_EXPIRE_TIME                = "TABLE_META_CACHE_EXPIRE_TIME";

    /**
     * 优化器和parser结果超时时间，单位毫秒，默认5分钟
     */
    public static final String OPTIMIZER_CACHE_EXPIRE_TIME                 = "OPTIMIZER_CACHE_EXPIRE_TIME";

    /**
     * 优化器和parser可缓存的数量，默认1000
     */
    public static final String OPTIMIZER_CACHE_SIZE                        = "OPTIMIZER_CACHE_SIZE";

    /**
     * 开启优先选择临时表模式，比如order by/group by，选择临时表缓存数据，默认为false，使用多链接缓存数据
     * 只有在ALLOW_TEMPORARY_TABLE参数设置为true时，该参数才会生效.
     */
    public static final String CHOOSE_TEMPORARY_TABLE                      = "CHOOSE_TEMPORARY_TABLE";

    /**
     * 是否允许全表扫描查询,默认为false
     */
    public static final String ALLOW_FULL_TABLE_SCAN                       = "ALLOW_FULL_TABLE_SCAN";

    /**
     * 如果这个值为true,则允许使用临时表。 而如果为空。或者为false,则不允许使用临时表。
     * 从性能和实际需求来说，默认值应该为false.也就是不允许使用临时表。
     */
    public static final String ALLOW_TEMPORARY_TABLE                       = "ALLOW_TEMPORARY_TABLE";

    /**
     * 是否强制开启/关闭streaming模式,默认为空值,也就是会选择自适应模式
     */
    public static final String CHOOSE_STREAMING                            = "CHOOSE_STREAMING";

    /**
     * 广播表是否开启多写
     */
    public static final String CHOOSE_BROADCAST_WRITE                      = "CHOOSE_BROADCAST_WRITE";

    /**
     * 创建cursor后是否立马执行
     */
    public static final String EXECUTE_QUERY_WHEN_CREATED                  = "EXECUTE_QUERY_WHEN_CREATED";

    /**
     * hbase的faimly/rowkey和表结构的映射配置文件
     */
    public static final String HBASE_MAPPING_FILE                          = "HBASE_MAPPING_FILE";

    /**
     * 执行jdbc fetch size
     */
    public static final String FETCH_SIZE                                  = "FETCH_SIZE";

    /**
     * 为每个连接都初始化一个线程池，用来做并行查询，默认为true
     */
    public static final String INIT_CONCURRENT_POOL_EVERY_CONNECTION       = "INIT_CONCURRENT_POOL_EVERY_CONNECTION";

    /**
     * 并行查询线程池大小
     */
    public static final String CONCURRENT_THREAD_SIZE                      = "CONCURRENT_THREAD_SIZE";

    /**
     * 并行查询最大线程数,在INIT_CONCURRENT_POOL_EVERY_CONNECTION模式下，避免产生过多的线程
     */
    public static final String MAX_CONCURRENT_THREAD_SIZE                  = "MAX_CONCURRENT_THREAD_SIZE";

    /**
     * 临时表的存放目录
     */
    public static final String TEMP_TABLE_DIR                              = "TEMP_TABLE_DIR";

    /**
     * 临时表允许占用的内存大小在JVM内存中占的百分比
     */
    public static final String TEMP_TABLE_CACHE_PERCENT                    = "TEMP_TABLE_CACHE_PERCENT";

    /**
     * 临时表最大记录条数
     */
    public static final String TEMP_TABLE_MAX_ROWS                         = "TEMP_TABLE_MAX_ROWS";

    /**
     * 临时表超过最大条数时，是否截断数据。
     */
    public static final String TEMP_TABLE_CUT_ROWS                         = "TEMP_TABLE_CUT_ROWS";

    /**
     * index nested loop最大执行时间，单位毫秒
     */
    public static final String INDEX_NESTED_LOOP_TIME_OUT                  = "INDEX_NESTED_LOOP_TIME_OUT";

    /**
     * index nested loop 最大迭代次数
     */
    public static final String MAX_INDEX_NESTED_LOOP_ITERATION_TIMES       = "MAX_INDEX_NESTED_LOOP_ITERATION_TIMES";

    /**
     * index nested loop每次发到右表的key的个数
     */
    public static final String COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP     = "COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP";

    /**
     * index nested loop中从右表返回的最大行数
     */
    public static final String MAX_ROW_RETURN_FROM_RIGHT_INDEX_NESTED_LOOP = "MAX_ROW_RETURN_FROM_RIGHT_INDEX_NESTED_LOOP";

    /**
     * Sequence表名
     */
    public static final String SEQUENCE_TABLE_NAME                         = "SEQUENCE_TABLE_NAME";

    /**
     * 是否使用sequence替换分布式表的auto_increment
     */
    public static final String PROCESS_AUTO_INCREMENT_BY_SEQUENCE          = "PROCESS_AUTO_INCREMENT_BY_SEQUENCE";

}
