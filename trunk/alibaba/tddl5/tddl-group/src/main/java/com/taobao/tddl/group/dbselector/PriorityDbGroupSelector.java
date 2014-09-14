package com.taobao.tddl.group.dbselector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.sorter.ExceptionSorter;
import com.taobao.tddl.group.exception.GroupNotAvaliableException;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;


/**
 * <pre>
 * 按优先级选择的selector 每次选择只从优先级最高的一组DB中选择，若都不可用，才继续在下一个优先级的DB组中选择,优先级相同的DB还用随机选择
 * 
 * 原始需求：TC要求在每个dbgroup中优先读备库，当备库不可用时，自动读主库 
 * 扩展需求：一主多备，优先随机读备库。当备库都不可用时，才读主库
 * 
 * 为了方便处理和接口一致，有如下要求： 
 * 1. 目前只支持读分优先级组 
 * 2. 一个权重推送的信息中，。。。 
 * 3. 一个数据源只能在一个优先级组中？
 * </pre>
 * 
 * @author linxuan
 */
public class PriorityDbGroupSelector extends AbstractDBSelector {

    private static final Logger logger = LoggerFactory.getLogger(PriorityDbGroupSelector.class);

    /**
     * 按优先级顺序存放数据库组。元素0优先级最高。每个EquityDbManager元素代表具有相同优先级的一组数据库
     */
    private EquityDbManager[]   priorityGroups;

    public PriorityDbGroupSelector(EquityDbManager[] priorityGroups){
        this.priorityGroups = priorityGroups;
        if (priorityGroups == null || priorityGroups.length == 0) {
            throw new IllegalArgumentException("EquityDbManager[] priorityGroups is null or empty");
        }
    }

    public DataSource select() {
        for (int i = 0; i < priorityGroups.length; i++) {
            DataSource ds = priorityGroups[i].select();
            if (ds != null) {
                return ds;
            }
        }
        return null;
    }

    public DataSourceWrapper get(String dsKey) {
        for (int i = 0; i < priorityGroups.length; i++) {
            DataSourceWrapper ds = priorityGroups[i].get(dsKey);
            if (ds != null) {
                return ds;
            }
        }
        return null;
    }

    /**
     * 取每个级别的weightKey和总的weightKey的交集，挨个设置
     */
    public void setWeight(Map<String, Integer> weightMap) {
        /*
         * for (int i = 0; i < priorityGroups.length; i++) { Map<String,
         * Integer> oldWeights = priorityGroups[i].getWeights(); Map<String,
         * Integer> newWeights = new HashMap<String,
         * Integer>(oldWeights.size()); for (Map.Entry<String, Integer> e :
         * weightMap.entrySet()) { if (oldWeights.containsKey(e.getKey())) {
         * newWeights.put(e.getKey(), e.getValue()); } }
         * priorityGroups[i].setWeightRandom(new WeightRandom(newWeights)); }
         */
    }

    private static class DataSourceTryerWrapper<T> implements DataSourceTryer<T> {

        private final List<SQLException> historyExceptions;
        private final DataSourceTryer<T> tryer;

        public DataSourceTryerWrapper(DataSourceTryer<T> tryer, List<SQLException> historyExceptions){
            this.tryer = tryer;
            this.historyExceptions = historyExceptions;
        }

        public T onSQLException(List<SQLException> exceptions, ExceptionSorter exceptionSorter, Object... args)
                                                                                                               throws SQLException {
            Exception last = exceptions.get(exceptions.size() - 1);
            if (last instanceof GroupNotAvaliableException) {
                if (exceptions.size() > 1) {
                    exceptions.remove(exceptions.size() - 1);
                }
                historyExceptions.addAll(exceptions);
                throw (GroupNotAvaliableException) last;
            } else {
                return tryer.onSQLException(exceptions, exceptionSorter, args);
            }
        }

        public T tryOnDataSource(DataSourceWrapper dsw, Object... args) throws SQLException {
            return tryer.tryOnDataSource(dsw, args);
        }
    };

    /**
     * 基于EquityDbManager的tryExecute实现，对用户的tryer做一个包装，在wrapperTryer.
     * onSQLException中
     * 检测到最后一个e是NoMoreDataSourceException时，不调原tryer的onSQLException, 转而重试其他优先级的
     */
    protected <T> T tryExecuteInternal(Map<DataSource, SQLException> failedDataSources, DataSourceTryer<T> tryer,
                                       int times, Object... args) throws SQLException {
        final List<SQLException> historyExceptions = new ArrayList<SQLException>(0);
        DataSourceTryer<T> wrapperTryer = new DataSourceTryerWrapper<T>(tryer, historyExceptions); // 移花接木

        for (int i = 0; i < priorityGroups.length; i++) {
            try {
                return priorityGroups[i].tryExecute(failedDataSources, wrapperTryer, times, args);
            } catch (GroupNotAvaliableException e) {
                logger.warn("NoMoreDataSource for retry for priority group " + i);
            }
        }
        // 所有的优先级组都不可用，则抛出异常
        return tryer.onSQLException(historyExceptions, exceptionSorter, args);
    }

    @Override
    public void setSupportRetry(boolean isSupportRetry) {
        for (int i = 0; i < priorityGroups.length; i++) {
            priorityGroups[i].setSupportRetry(isSupportRetry);
        }
        this.isSupportRetry = isSupportRetry;
    }

    public void setReadable(boolean readable) {
        for (int i = 0; i < priorityGroups.length; i++) {
            priorityGroups[i].setReadable(readable);
        }

        this.readable = readable;
    }

    /*
     * public DataSource[] getDataSourceArray() { List<DataSource> dataSources =
     * new ArrayList<DataSource>(); for (EquityDbManager e : priorityGroups) {
     * for (DataSource ds : e.getDataSourceArray()) dataSources.add(ds); }
     * return dataSources.toArray(new DataSource[0]); }
     */

    public Map<String, DataSource> getDataSources() {
        throw new NotSupportException("getDataSources()");
    }

    protected DataSourceHolder findDataSourceWrapperByIndex(int dataSourceIndex) {
        for (int i = 0; i < priorityGroups.length; i++) {
            DataSourceHolder holder = priorityGroups[i].findDataSourceWrapperByIndex(dataSourceIndex);
            if (holder != null) {
                return holder;
            }

        }
        return null;
    }
}
