package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.plan.query.IGetSequence;

/**
 * 兼容以前corona的获取sequnece逻辑
 * 
 * @author jianghang 2014-5-19 下午3:55:10
 * @since 5.1.0
 */
public class GetSequence extends DataNodeExecutor<IGetSequence> implements IGetSequence {

    private String name;
    private int    count;

    public GetSequence(String name, int count){
        this.name = name;
        this.count = count;
    }

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        String table = (name != null) ? "FROM " + name : "";
        String where = (count > 0) ? "WHERE CORONA_SEQ_COUNT = " + count : "";
        return "SELECT CORONA_NEXT_VAL" + table + where;
    }

    @Override
    public IGetSequence copy() {
        GetSequence get = new GetSequence(this.name, this.count);
        return get;
    }

    public void accept(PlanVisitor visitor) {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

}
