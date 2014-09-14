package com.taobao.tddl.executor.handler;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IGetSequence;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class GetSequenceHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ArrayResultCursor result = new ArrayResultCursor("SEQUENCE", executionContext);
        result.addColumn("DRDS_SEQ_VAL", DataType.BigIntegerType);
        result.initMeta();
        ISequenceManager seqManager = ExecutorContext.getContext().getSeqeunceManager();
        IGetSequence getSequence = (IGetSequence) executor;
        String seqName = getSequence.getName();
        if (StringUtils.isEmpty(getSequence.getName())) {
            seqName = "default";
        }
        if (getSequence.getCount() <= 1) {
            result.addRow(new Object[] { seqManager.nextValue(seqName) });
        } else {
            for (int i = 0; i < getSequence.getCount(); i++) {
                result.addRow(new Object[] { seqManager.nextValue(seqName) });
            }
        }
        return result;
    }
}
