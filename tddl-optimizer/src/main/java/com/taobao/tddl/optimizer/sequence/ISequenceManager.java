package com.taobao.tddl.optimizer.sequence;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * @author mengshi.sunmengshi 2014年4月28日 下午2:51:04
 * @since 5.1.0
 */
public interface ISequenceManager extends Lifecycle {

    public String AUTO_SEQ_PREFIX = "AUTO_SEQ_";

    /**
     * @param seqName
     * @return
     */
    Long nextValue(String seqName);

    /**
     * @param seqName
     * @param batchSize
     * @return
     */
    Long nextValue(String seqName, int batchSize);

}
