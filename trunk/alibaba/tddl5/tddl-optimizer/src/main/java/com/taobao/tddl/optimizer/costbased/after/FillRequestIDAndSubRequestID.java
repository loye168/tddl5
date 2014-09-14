package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.utils.AddressUtils;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.UniqIdGen;

/**
 * 添加id 不会改变结构
 * 
 * @author Whisper
 */
public class FillRequestIDAndSubRequestID implements QueryPlanOptimizer {

    String hostname = "";

    public FillRequestIDAndSubRequestID(){
        hostname = AddressUtils.getHostIp() + "_" + System.currentTimeMillis();
    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        if (dne instanceof IQueryTree) {
            fillRequestIdAndSubRequestIdFromRoot(dne, 1);
        } else {
            dne.setSubRequestId(1l);
            dne.setRequestId(UniqIdGen.genRequestID());
            dne.setRequestHostName(hostname);
        }
        return dne;
    }

    public long fillRequestIdAndSubRequestIdFromRoot(IDataNodeExecutor qc, long subRequestId) {
        qc.setSubRequestId(subRequestId);
        qc.setRequestId(UniqIdGen.genRequestID());
        qc.setRequestHostName(hostname);

        if (qc instanceof IQuery && ((IQuery) qc).getSubQuery() != null) {
            subRequestId = this.fillRequestIdAndSubRequestIdFromRoot(((IQuery) qc).getSubQuery(), subRequestId + 1);
        } else if (qc instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) qc).getSubNodes()) {
                subRequestId = this.fillRequestIdAndSubRequestIdFromRoot(sub, subRequestId + 1);
            }
        } else if (qc instanceof IJoin) {
            subRequestId = this.fillRequestIdAndSubRequestIdFromRoot(((IJoin) qc).getLeftNode(), subRequestId + 1);
            subRequestId = this.fillRequestIdAndSubRequestIdFromRoot(((IJoin) qc).getRightNode(), subRequestId + 1);
        }

        return subRequestId;
    }

}
