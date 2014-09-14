package com.taobao.tddl.optimizer.core.expression.bean;

import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.sequence.SequenceManagerProxy;

/**
 * sequence.nextval获取的实现
 * 
 * @author jianghang 2014-4-28 下午3:39:04
 * @since 5.1.0
 */
public class SequenceVal extends BindVal implements ISequenceVal {

    private Object name;
    private Long   batchMaxval = -1L;

    public SequenceVal(Object name){
        super(0);
        this.name = name;
    }

    @Override
    public Object assignment(Parameters parameterSettings) {
        Object key = null;
        if (name instanceof IBindVal) {
            key = ((IBindVal) name).assignment(parameterSettings);
        } else if (name instanceof ISelectable) {
            key = ((IBindVal) name).assignment(parameterSettings);
        } else {
            key = name;
        }

        String k = DataType.StringType.convertFrom(key);
        if (parameterSettings != null && parameterSettings.isBatch()) {
            if (batchMaxval < 0) {
                batchMaxval = SequenceManagerProxy.getInstance().nextValue(k, parameterSettings.getBatchSize());
                ThreadLocalMap.put(IDataNodeExecutor.LAST_SEQUENCE_VAL, batchMaxval);// 只记录最大的seqId
            }
            // 返回value用户做rule计算
            value = batchMaxval - parameterSettings.getBatchSize() + parameterSettings.getBatchIndex() + 1;
            // 将value加入到绑定变量中
            parameterSettings.getCurrentParameter().put(index,
                new ParameterContext(ParameterMethod.setLong, new Object[] { index, value }));
            return this;
        } else {
            Long nextval = SequenceManagerProxy.getInstance().nextValue(k);
            ThreadLocalMap.put(IDataNodeExecutor.LAST_SEQUENCE_VAL, nextval);// 使用thread变量
            return nextval;
        }
    }

    @Override
    public void setOriginIndex(int index) {
        this.index = index;
    }

    @Override
    public ISequenceVal copy() {
        Object newName = null;
        if (name instanceof IBindVal) {
            newName = ((IBindVal) name).copy();
        } else if (name instanceof ISelectable) {
            newName = ((IBindVal) name).copy();
        } else {
            newName = name;
        }

        SequenceVal val = new SequenceVal(newName);
        val.setOriginIndex(index);
        return val;
    }

    @Override
    public String toString() {
        return name.toString() + ".NEXTVAL";
    }

}
