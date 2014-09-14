package com.taobao.tddl.optimizer.core.plan;

import com.taobao.tddl.optimizer.core.CanVisit;

/**
 * 描述执行计划
 * 
 * @since 5.0.0
 */
public interface IDataNodeExecutor<RT extends IDataNodeExecutor> extends CanVisit {

    public String USE_LAST_DATA_NODE = "_USE_LAST_DATA_NODE_";

    public String LAST_SEQUENCE_VAL  = "LAST_SEQUENCE_VAL";

    public String DUAL_GROUP         = "DUAL_GROUP";

    public static enum ExplainMode {
        /** 逻辑模式,不输出物理表 */
        LOGIC,
        /** 简略模式,输出物理表,会对LOGIC相同的做合并 */
        SIMPLE,
        /** 详细模式，在simple模式下，输出列信息 */
        DETAIL;

        public boolean isLogic() {
            return this == LOGIC || isSimple();
        }

        public boolean isSimple() {
            return this == SIMPLE || isDetail();
        }

        public boolean isDetail() {
            return this == DETAIL;
        }

    }

    /**
     * 设定当前的查询在哪里执行
     * 
     * @param targetNode 执行的逻辑节点名字
     * @return ..
     */
    RT executeOn(String targetNode);

    /**
     * 获取当前执行节点在哪里执行
     * 
     * @return
     */
    String getDataNode();

    /**
     * 设定当前节点是要求一个实时一致性读写读取。
     * 
     * @param consistent
     * @return
     */
    RT setConsistent(boolean consistent);

    /**
     * 当前查询是否要求是一个实时一致性读写请求。
     * 
     * @return
     */
    boolean getConsistent();

    /**
     * 设置发起请求的源hostName.可选，主要是结合requestID 两个属性来唯一的标志这个请求用的。
     * 
     * @param srcHostName
     * @return
     */
    RT setRequestHostName(String srcHostName);

    /**
     * 获取请求的源hostName,主要是结合requestId两个属性来唯一的标志这个请求。
     * 用于让所有的机器都能追踪到这个请求，并且能够估算请求运行的百分比。
     * 
     * @return
     */
    String getRequestHostName();

    /**
     * 设置这个请求的全局requestID
     * 
     * @param requestId
     * @return
     */
    RT setRequestId(Long requestId);

    /**
     * 获取这个请求的requestID
     * 
     * @return
     */
    Long getRequestId();

    /**
     * 设置子请求ID. 用于标记一个查询是否能够被trace.如果能够被trace
     * 那么就应该设置hostName,全局requestID并且设置subRequestID
     * 
     * @param subRequestID
     * @return
     */
    RT setSubRequestId(Long subRequestID);

    /**
     * 获取子请求ID 用于标记一个查询是否能够被trace.如果能够被trace
     * 那么就应该设置hostName,全局requestID并且设置subRequestID
     * 
     * @return
     */
    Long getSubRequestId();

    /**
     * 用于输出带缩进的字符串
     * 
     * @param inden
     * @return
     */
    public String toStringWithInden(int inden, ExplainMode mode);

    public Integer getThread();

    /**
     * 表明一个建议的用于执行该节点的线程id
     * 
     * @param thread
     */
    public RT setThread(Integer thread);

    public Object getExtra();

    public RT setExtra(Object obj);

    public boolean isUseBIO();

    public RT setUseBIO(boolean useBIO);

    public boolean isStreaming();

    public RT setStreaming(boolean streaming);

    public String getSql();

    public RT setSql(String sql);

    public boolean lazyLoad();

    public void setLazyLoad(boolean lazyLoad);

    public boolean isExistSequenceVal();

    public void setExistSequenceVal(boolean existSequenceVal);

    public Long getLastSequenceVal();

    public void setLastSequenceVal(Long sequenceVal);

    public ExplainMode getExplainMode();

    public void setExplainMode(ExplainMode mode);

    public boolean isExplain();

    // ------------------复制----------------

    public RT copy();

}
