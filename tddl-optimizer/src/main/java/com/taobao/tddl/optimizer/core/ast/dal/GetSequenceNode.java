package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.GetSequence;

/**
 * 兼容以前corona的获取sequnece逻辑
 * 
 * @author jianghang 2014-5-19 下午3:54:14
 * @since 5.1.0
 */
public class GetSequenceNode extends ASTNode {

    private String name;
    private int    count;

    public GetSequenceNode(String name, int count){
        this.name = name;
        this.count = count;
    }

    @Override
    public void build() {
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        GetSequence get = new GetSequence(this.name, this.count);
        get.executeOn(this.getDataNode());
        return get;
    }

    @Override
    public void assignment(Parameters parameterSettings) {

    }

    @Override
    public boolean isNeedBuild() {
        return false;
    }

    @Override
    public String toString(int inden, int shareIndex) {
        String table = (name != null) ? "FROM " + name : "";
        String where = (count > 0) ? "WHERE CORONA_SEQ_COUNT = " + count : "";
        return "SELECT CORONA_NEXT_VAL" + table + where;
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public GetSequenceNode copy() {
        return deepCopy();
    }

    @Override
    public GetSequenceNode copySelf() {
        return deepCopy();
    }

    @Override
    public GetSequenceNode deepCopy() {
        GetSequenceNode get = new GetSequenceNode(this.name, this.count);
        get.executeOn(this.getDataNode());
        return get;
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
