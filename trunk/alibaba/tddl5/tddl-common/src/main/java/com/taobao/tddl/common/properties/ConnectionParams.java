package com.taobao.tddl.common.properties;

import java.util.HashMap;
import java.util.Map;

public class ConnectionParams {

    public static final Map<String, ConfigParam> SUPPORTED_PARAMS                            = new HashMap<String, ConfigParam>();

    public static final BooleanConfigParam       OPTIMIZER_CACHE                             = new BooleanConfigParam(ConnectionProperties.OPTIMIZER_CACHE,
                                                                                                 true, // default
                                                                                                 false);                          // mutable
                                                                                                                                   // forReplication

    /**
     * 是否选取索引，默认为true
     */
    public final static BooleanConfigParam       CHOOSE_INDEX                                = new BooleanConfigParam(ConnectionProperties.CHOOSE_INDEX,
                                                                                                 true, // default
                                                                                                 false);                          // mutable
                                                                                                                                   // forReplication

    /**
     * 是否选择最优join顺序, 默认为false
     */
    public final static BooleanConfigParam       CHOOSE_JOIN                                 = new BooleanConfigParam(ConnectionProperties.CHOOSE_JOIN,
                                                                                                 false, // default
                                                                                                 false);                          // mutable
                                                                                                                                   // forReplication

    /**
     * 是否将or条件转化为index merge，默认为false
     */
    public final static BooleanConfigParam       CHOOSE_INDEX_MERGE                          = new BooleanConfigParam(ConnectionProperties.CHOOSE_INDEX_MERGE,
                                                                                                 false, // default
                                                                                                 false);                          // mutable
                                                                                                                                   // forReplication

    /**
     * 智能优化join merge join，默认为true
     */
    public final static BooleanConfigParam       JOIN_MERGE_JOIN_JUDGE_BY_RULE               = new BooleanConfigParam(ConnectionProperties.JOIN_MERGE_JOIN_JUDGE_BY_RULE,
                                                                                                 true, // default
                                                                                                 false);                          // mutable

    /**
     * 是否强制优化成join merge join，默认为false
     */
    public final static BooleanConfigParam       JOIN_MERGE_JOIN                             = new BooleanConfigParam(ConnectionProperties.JOIN_MERGE_JOIN,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    /**
     * 是否展开Merge Join Merge，默认为false
     */
    public final static BooleanConfigParam       MERGE_EXPAND                                = new BooleanConfigParam(ConnectionProperties.MERGE_EXPAND,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    /**
     * 是否设置Merge并行执行，默认为true
     */
    public final static BooleanConfigParam       MERGE_CONCURRENT                            = new BooleanConfigParam(ConnectionProperties.MERGE_CONCURRENT,
                                                                                                 true, // default
                                                                                                 false);                          // mutable

    /**
     * 是否展开in，把in转换成or,默认不展开
     */
    public final static BooleanConfigParam       EXPAND_IN                                   = new BooleanConfigParam(ConnectionProperties.EXPAND_IN,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    /**
     * 是否展开 a and (b = 1 or b = 2),默认不展开
     */
    public final static BooleanConfigParam       EXPAND_OR                                   = new BooleanConfigParam(ConnectionProperties.EXPAND_OR,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    /**
     * 表的meta超时时间，单位毫秒，默认5分钟
     */
    public static final LongConfigParam          TABLE_META_CACHE_EXPIRE_TIME                = new LongConfigParam(ConnectionProperties.TABLE_META_CACHE_EXPIRE_TIME,
                                                                                                 null, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(300 * 1000),// default
                                                                                                 true);                           // mutable

    /**
     * 优化器和parser结果超时时间，单位毫秒，默认5分钟
     */
    public static final LongConfigParam          OPTIMIZER_CACHE_EXPIRE_TIME                 = new LongConfigParam(ConnectionProperties.OPTIMIZER_CACHE_EXPIRE_TIME,
                                                                                                 null, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(300 * 1000),// default
                                                                                                 true);                           // mutable

    /**
     * 优化器和parser可缓存的数量，默认1000
     */
    public static final LongConfigParam          OPTIMIZER_CACHE_SIZE                        = new LongConfigParam(ConnectionProperties.OPTIMIZER_CACHE_SIZE,
                                                                                                 null, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(1000),
                                                                                                 true);

    /**
     * 如果这个值为true,则强制开启允许全表扫描查询。而如果为空。或者为false,则基于规则判断是否开启全表扫描
     */
    public static final BooleanConfigParam       ALLOW_FULL_TABLE_SCAN                       = new BooleanConfigParam(ConnectionProperties.ALLOW_FULL_TABLE_SCAN,
                                                                                                 false, // default
                                                                                                 false);

    /**
     * 如果这个值为true,则允许使用临时表。 而如果为空。或者为false,则不允许使用临时表。
     * 从性能和实际需求来说，默认值应该为false.也就是不允许使用临时表。
     */
    public static final BooleanConfigParam       ALLOW_TEMPORARY_TABLE                       = new BooleanConfigParam(ConnectionProperties.ALLOW_TEMPORARY_TABLE,
                                                                                                 false, // default
                                                                                                 false);                          // mutable
    /**
     * 在允许临时表的模式下，比如order by/group
     * by，选择为true时，才采用临时表缓存数据(可减少链接使用)，默认为false，使用多链接缓存数据
     */
    public static final BooleanConfigParam       CHOOSE_TEMPORARY_TABLE                      = new BooleanConfigParam(ConnectionProperties.CHOOSE_TEMPORARY_TABLE,
                                                                                                 false, // default
                                                                                                 false);

    /**
     * 是否开启广播表多写
     */
    public static final BooleanConfigParam       CHOOSE_BROADCAST_WRITE                      = new BooleanConfigParam(ConnectionProperties.CHOOSE_BROADCAST_WRITE,
                                                                                                 true, // default
                                                                                                 true);

    /**
     * 创建cursor后是否立马执行
     */
    public static final BooleanConfigParam       EXECUTE_QUERY_WHEN_CREATED                  = new BooleanConfigParam(ConnectionProperties.EXECUTE_QUERY_WHEN_CREATED,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    public static final StringConfigParam        HBASE_MAPPING_FILE                          = new StringConfigParam(ConnectionProperties.HBASE_MAPPING_FILE,
                                                                                                 null,
                                                                                                 false);

    /**
     * 执行jdbc fetch size
     */
    public static final LongConfigParam          FETCH_SIZE                                  = new LongConfigParam(ConnectionProperties.FETCH_SIZE,
                                                                                                 null, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(0),// default
                                                                                                 true);                           // mutable

    /**
     * 为每个连接都初始化一个线程池，用来做并行查询，默认为true
     */
    public static final BooleanConfigParam       INIT_CONCURRENT_POOL_EVERY_CONNECTION       = new BooleanConfigParam(ConnectionProperties.INIT_CONCURRENT_POOL_EVERY_CONNECTION,
                                                                                                 true, // default
                                                                                                 false);                          // mutable

    /**
     * 并行查询线程池大小
     */
    public static final IntConfigParam           CONCURRENT_THREAD_SIZE                      = new IntConfigParam(ConnectionProperties.CONCURRENT_THREAD_SIZE,
                                                                                                 Integer.valueOf(1), // min
                                                                                                 null, // max
                                                                                                 Integer.valueOf(8),// default
                                                                                                 false);                          // mutable

    /**
     * 临时表的存放目录
     */
    public static final StringConfigParam        TEMP_TABLE_DIR                              = new StringConfigParam(ConnectionProperties.TEMP_TABLE_DIR,
                                                                                                 ".",
                                                                                                 false);

    /**
     * 临时表允许占用的内存大小在JVM内存中占的百分比
     */
    public static final IntConfigParam           TEMP_TABLE_CACHE_PERCENT                    = new IntConfigParam(ConnectionProperties.TEMP_TABLE_CACHE_PERCENT,
                                                                                                 Integer.valueOf(1), // min
                                                                                                 Integer.valueOf(100), // max
                                                                                                 Integer.valueOf(20),// default
                                                                                                 false);                          // mutable

    /**
     * 临时表最大记录条数
     */
    public static final LongConfigParam          TEMP_TABLE_MAX_ROWS                         = new LongConfigParam(ConnectionProperties.TEMP_TABLE_MAX_ROWS,
                                                                                                 null, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(100000),// default
                                                                                                 true);                           // mutable

    /**
     * 临时表超过最大条数时，是否截断数据。
     */
    public static final BooleanConfigParam       TEMP_TABLE_CUT_ROWS                         = new BooleanConfigParam(ConnectionProperties.TEMP_TABLE_CUT_ROWS,
                                                                                                 false, // default
                                                                                                 false);                          // mutable

    /**
     * index nested loop最大执行时间，单位毫秒 默认0，不超时
     */
    public static final LongConfigParam          INDEX_NESTED_LOOP_TIME_OUT                  = new LongConfigParam(ConnectionProperties.INDEX_NESTED_LOOP_TIME_OUT,
                                                                                                 0L, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(0),// default
                                                                                                 true);                           // mutable

    /**
     * index nested loop 最大迭代次数,默认0，无限制
     */
    public static final LongConfigParam          MAX_INDEX_NESTED_LOOP_ITERATION_TIMES       = new LongConfigParam(ConnectionProperties.MAX_INDEX_NESTED_LOOP_ITERATION_TIMES,
                                                                                                 0L, // min
                                                                                                 null, // max
                                                                                                 Long.valueOf(0),// default
                                                                                                 true);                           // mutable

    /**
     * index nested loop每次发到右表的key的个数
     */
    public static final IntConfigParam           COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP     = new IntConfigParam(ConnectionProperties.COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP,
                                                                                                 20, // min
                                                                                                 null, // max
                                                                                                 Integer.valueOf(20),// default
                                                                                                 true);                           // mutable

    /**
     * index nested loop中从右表返回的最大行数
     */

    public static final IntConfigParam           MAX_ROW_RETURN_FROM_RIGHT_INDEX_NESTED_LOOP = new IntConfigParam(ConnectionProperties.MAX_ROW_RETURN_FROM_RIGHT_INDEX_NESTED_LOOP,
                                                                                                 20, // min
                                                                                                 null, // max
                                                                                                 Integer.valueOf(5000),// default
                                                                                                 true);                           // mutable

    /**
     * Sequence表名
     */
    public static final StringConfigParam        SEQUENCE_TABLE_NAME                         = new StringConfigParam(ConnectionProperties.SEQUENCE_TABLE_NAME,
                                                                                                 "sequence",
                                                                                                 false);

    /**
     * 是否使用sequence替换分布式表的auto_increment
     */
    public static final BooleanConfigParam       PROCESS_AUTO_INCREMENT_BY_SEQUENCE          = new BooleanConfigParam(ConnectionProperties.PROCESS_AUTO_INCREMENT_BY_SEQUENCE,
                                                                                                 true,
                                                                                                 true);

    public static void addSupportedParam(ConfigParam param) {
        SUPPORTED_PARAMS.put(param.getName(), param);
    }
}
