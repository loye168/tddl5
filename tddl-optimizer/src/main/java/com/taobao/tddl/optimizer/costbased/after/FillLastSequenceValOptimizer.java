package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 添加下lastsequenceVal
 * 
 * @author jianghang 2014-4-28 下午11:44:38
 * @since 5.1.0
 */
public class FillLastSequenceValOptimizer implements QueryPlanOptimizer {

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        if (dne instanceof IQueryTree) {
            fillFromRoot(dne);
        } else {
            dne.setLastSequenceVal(getLastSequneceVal(dne));
        }
        return dne;
    }

    public void fillFromRoot(IDataNodeExecutor qc) {
        qc.setLastSequenceVal(getLastSequneceVal(qc));
        if (qc instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) qc).getSubNodes()) {
                fillFromRoot(sub);
            }
        } else if (qc instanceof IQuery && ((IQuery) qc).getSubQuery() != null) {
            fillFromRoot(((IQuery) qc).getSubQuery());
        } else if (qc instanceof IJoin) {
            fillFromRoot(((IJoin) qc).getLeftNode());
            fillFromRoot(((IJoin) qc).getRightNode());
        } else {
            qc.setLastSequenceVal(getLastSequneceVal(qc));
        }
    }

    private Long getLastSequneceVal(IDataNodeExecutor dne) {
        if (dne.isExistSequenceVal()) {
            return (Long) ThreadLocalMap.get(IDataNodeExecutor.LAST_SEQUENCE_VAL);
        } else {
            return null;
        }
    }

}
