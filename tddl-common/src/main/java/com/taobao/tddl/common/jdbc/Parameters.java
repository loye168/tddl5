package com.taobao.tddl.common.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * @author mengshi.sunmengshi 2014年3月5日 下午2:26:36
 * @since 5.1.0
 */
public class Parameters {

    private List<Map<Integer, ParameterContext>> batchParams  = null;
    private boolean                              batch        = false;
    private Map<Integer, ParameterContext>       params       = new HashMap<Integer, ParameterContext>();
    private int                                  batchSize    = 0;                                       // batch的数量
    private int                                  batchIndex   = 0;                                       // 记录一下遍历过程中的index
    private AtomicInteger                        sequenceSize = new AtomicInteger(0);                    // seq列的数量

    public Parameters(){
    }

    public Parameters(Map<Integer, ParameterContext> currentParameter, boolean isBatch){
        this.params = currentParameter;
        this.batch = isBatch;
    }

    public Map<Integer, ParameterContext> getCurrentParameter() {
        return params;
    }

    public Map<Integer, ParameterContext> getFirstParameter() {
        if (!batch) {
            return params;
        }
        return batchParams.get(0);
    }

    /**
     * 返回批处理的参数，如果当前非批处理，返回单条记录
     */
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        if (isBatch() && this.batchParams != null) {
            return this.batchParams;
        } else {
            return Arrays.asList(params);
        }
    }

    public Parameters cloneByBatchIndex(int batchIndex) {
        List<Map<Integer, ParameterContext>> batchs = getBatchParameters();
        if (batchIndex >= batchs.size()) {
            throw new IllegalArgumentException("batchIndex is invalid");
        }

        Parameters parameters = new Parameters(batchs.get(batchIndex), isBatch());
        parameters.setBatchSize(batchSize);
        parameters.setBatchIndex(batchIndex);
        parameters.setSequenceSize(sequenceSize);
        return parameters;
    }

    public void addBatch() {
        if (batchParams == null) {
            batchParams = new ArrayList();
        }

        batchParams.add(this.params);
        batchSize = batchParams.size();
        params = new HashMap();
        this.batch = true;
    }

    public boolean isBatch() {
        return this.batch;
    }

    public void setBatchIndex(int batchIndex) {
        this.batchIndex = batchIndex;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public AtomicInteger getSequenceSize() {
        return sequenceSize;
    }

    public void setSequenceSize(AtomicInteger sequenceSize) {
        this.sequenceSize = sequenceSize;
    }

    public static void setParameters(PreparedStatement ps, Map<Integer, ParameterContext> parameterSettings)
                                                                                                            throws SQLException {
        ParameterMethod.setParameters(ps, parameterSettings);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
