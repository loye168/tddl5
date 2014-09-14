package com.taobao.tddl.optimizer.core.ast;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.costbased.SequencePreProcessor;
import com.taobao.tddl.optimizer.costbased.SubQueryPreProcessor;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * DML操作树
 * 
 * @since 5.0.0
 */
public abstract class DMLNode<RT extends DMLNode> extends ASTNode<RT> {

    protected static final Logger logger               = LoggerFactory.getLogger(DMLNode.class);
    protected List<ISelectable>   columns;
    protected List<Object>        values;
    protected boolean             isMultiValues        = false;
    protected List<List<Object>>  multiValues;
    protected boolean             processAutoIncrement = true;
    // 直接依赖为tableNode，如果涉及多库操作，会是一个Merge下面挂多个DML
    protected TableNode           table                = null;
    protected Parameters          parameterSettings    = null;
    protected boolean             needBuild            = true;
    protected List<Integer>       batchIndexs          = new ArrayList<Integer>();
    protected boolean             ignore               = false;
    protected boolean             lowPriority          = false;
    protected boolean             highPriority         = false;
    protected boolean             delayed              = false;
    protected boolean             quick                = false;
    // insert into ... select
    protected QueryTreeNode       selectNode;

    public DMLNode(){
        super();
    }

    public boolean isIgnore() {
        return ignore;
    }

    public DMLNode setIgnore(boolean ignore) {
        this.ignore = ignore;
        return this;
    }

    public boolean isLowPriority() {
        return lowPriority;
    }

    public DMLNode setLowPriority(boolean lowPriority) {
        this.lowPriority = lowPriority;
        return this;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    public DMLNode setHighPriority(boolean highPriority) {
        this.highPriority = highPriority;
        return this;
    }

    public boolean isDelayed() {
        return delayed;
    }

    public DMLNode setDelayed(boolean delayed) {
        this.delayed = delayed;

        return this;
    }

    public boolean isQuick() {
        return quick;
    }

    public DMLNode setQuick(boolean quick) {
        this.quick = quick;

        return this;
    }

    public DMLNode(TableNode table){
        this.table = table;
    }

    public DMLNode setParameterSettings(Parameters parameterSettings) {
        this.parameterSettings = parameterSettings;
        return this;
    }

    public TableNode getNode() {
        return this.table;
    }

    public DMLNode setNode(TableNode table) {
        this.table = table;
        return this;
    }

    public DMLNode setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getColumns() {
        return this.columns;
    }

    public DMLNode setValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public List<Object> getValues() {
        return this.values;
    }

    public TableMeta getTableMeta() {
        return getNode().getTableMeta();
    }

    @Override
    public boolean isNeedBuild() {
        return needBuild;
    }

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

    public boolean isMultiValues() {
        return isMultiValues;
    }

    public DMLNode setMultiValues(boolean isMutiValues) {
        this.isMultiValues = isMutiValues;
        return this;
    }

    public List<List<Object>> getMultiValues() {
        return multiValues;
    }

    public DMLNode setMultiValues(List<List<Object>> multiValues) {
        this.multiValues = multiValues;
        return this;
    }

    public List<Object> getValues(int index) {
        if (this.isMultiValues) {
            if (this.multiValues != null) {
                return this.multiValues.get(index);
            } else {
                return null;
            }
        }

        return this.values;
    }

    public int getMultiValuesSize() {
        if (this.isMultiValues) {
            return this.multiValues.size();
        } else {
            return 1;
        }
    }

    public boolean processAutoIncrement() {
        return processAutoIncrement;
    }

    @Override
    public void build() {
        if (this.table != null) {
            table.build();
        }

        if (this.selectNode != null) {
            selectNode.build();
        }

        // 先判断是否需要needAutoIncrement
        boolean needAutoIncrement = false;
        ISelectable autoIncrementColumn = null;
        if (processAutoIncrement()) {
            for (ColumnMeta res : getNode().getTableMeta().getAllColumns()) {
                if (res.isAutoIncrement()) {
                    needAutoIncrement = true;
                    autoIncrementColumn = OptimizerUtils.columnMetaToIColumn(res);
                }
            }
        }

        if (columns == null || columns.isEmpty()) { // 如果字段为空，默认为所有的字段数据的,比如insert所有字段
            columns = OptimizerUtils.columnMetaListToIColumnList(this.getTableMeta().getAllColumns(),
                this.getTableMeta().getTableName());
            needAutoIncrement = false;
        } else {
            for (ISelectable column : columns) {
                ColumnMeta res = getNode().getTableMeta().getColumn(column.getColumnName());
                if (res == null) {
                    throw new IllegalArgumentException("column: " + column.getFullName() + " is not existed in "
                                                       + this.getNode().getName());
                }
                column.setDataType(res.getDataType());
                column.setTableName(res.getTableName());
                column.setAutoIncrement(res.isAutoIncrement());

                if (column.isAutoIncrement()) {
                    needAutoIncrement = false;
                }
            }

            if (needAutoIncrement) { // 添加自增列
                columns.add(autoIncrementColumn);
            }
        }

        boolean isMatch = true;
        if (isMultiValues) {
            for (List<Object> vs : multiValues) {
                if (needAutoIncrement) { // 添加自增列
                    vs.add(SequencePreProcessor.buildSequenceVal(autoIncrementColumn));
                }

                for (Object v : vs) {
                    if (v instanceof ISequenceVal) {
                        existSequenceVal = true;
                    }
                }
                isMatch &= (vs.size() == columns.size());
            }
        } else if (values != null) {
            if (needAutoIncrement) { // 添加自增列
                values.add(SequencePreProcessor.buildSequenceVal(autoIncrementColumn));
            }

            isMatch &= (values.size() == columns.size());
            for (Object v : values) {
                if (v instanceof ISequenceVal) {
                    existSequenceVal = true;
                }
            }
        } else if (selectNode != null) {
            List<ISelectable> selectColumns = selectNode.getColumnsSelected();
            // insert into ... select语法，检查下列是否匹配
            if (processAutoIncrement()) {
                // 分库分表情况，不允许出现insert into table(id) ... select null列的情况
                int index = -1;
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(0).isAutoIncrement()) {
                        index = i;
                        break;
                    }
                }

                if (index >= 0) {
                    if (selectColumns.size() > index) {
                        ISelectable column = selectColumns.get(index);
                        if (column instanceof IBooleanFilter
                            && ((IBooleanFilter) column).getColumn() instanceof NullValue) {
                            throw new OptimizerException("insert into select not support select auto_increment is null");
                        }
                    }
                }
            }

            List<String> shardColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(getTableMeta().getTableName());
            if (!shardColumns.isEmpty()) {
                // 校验下insert/select中的分区键不会做修改，需要保持一致
                for (String shardColumn : shardColumns) {
                    int index = -1;
                    for (int i = 0; i < columns.size(); i++) {
                        ISelectable column = columns.get(i);
                        if (column.getColumnName().equals(shardColumn)) {// 对应的分库键
                            index = i;
                            break;
                        }
                    }

                    if (index == -1) {
                        throw new OptimizerException("parition column " + shardColumn + " is not found");
                    }

                    if (selectColumns.size() > index) {
                        ISelectable column = selectColumns.get(index);
                        if (!(column instanceof IColumn)) {// 对应的分库键的列，必须是column列，不能是常量或者是函数
                            throw new OptimizerException("insert into select only support select is parition column");
                        }
                    }
                }
            }

            isMatch &= (columns.size() == selectNode.getColumnsSelected().size());
        }

        if (!isMatch) {
            throw new OptimizerException("Column count doesn't match value count");
        }

        for (ISelectable s : this.getColumns()) {
            ISelectable res = null;
            for (Object obj : table.getColumnsReferedForParent()) {
                ISelectable querySelected = (ISelectable) obj;
                if (s.isSameName(querySelected)) { // 尝试查找对应的字段信息
                    res = querySelected;
                    break;
                }
            }

            if (res == null) {
                throw new IllegalArgumentException("column: " + s.getColumnName() + " is not existed in "
                                                   + table.getName());
            }
            s.setDataType(res.getDataType());
        }

        if (isMultiValues) {
            for (List<Object> vs : multiValues) {
                convertTypeToSatifyColumnMeta(this.getColumns(), vs);
            }
        } else if (values != null) {
            convertTypeToSatifyColumnMeta(this.getColumns(), values);
        }
    }

    /**
     * 尝试根据字段类型进行value转化
     */
    protected List<Object> convertTypeToSatifyColumnMeta(List<ISelectable> cs, List<Object> vs) {
        for (int i = 0; i < vs.size(); i++) {
            ISelectable c = cs.get(i);
            Object v = vs.get(i);
            if (c.isAutoIncrement()) {
                // 自增id必须是个具体的值或者values()函数
                if (v instanceof IColumn) {
                    throw new OptimizerException("unkonw sequence syntax : " + v.toString());
                }
            }
            vs.set(i, OptimizerUtils.convertType(v, c.getDataType()));
        }
        return vs;
    }

    @Override
    public RT executeOn(String dataNode) {
        super.executeOn(dataNode);
        return (RT) this;
    }

    @Override
    public void assignment(Parameters parameterSettings) {
        if (table != null) {
            table.assignment(parameterSettings);
        }

        if (selectNode != null) {
            selectNode.assignment(parameterSettings);
        }

        if (values != null) {
            this.setValues(assignmentValues(values, parameterSettings));
        }

        if (multiValues != null) {
            List<List<Object>> multiValues = new ArrayList<List<Object>>(this.multiValues.size());
            for (List<Object> values : this.multiValues) {
                multiValues.add(assignmentValues(values, parameterSettings));
            }

            this.setMultiValues(multiValues);
        }
    }

    /**
     * 尝试根据字段类型进行value转化
     */
    protected List<Object> assignmentValues(List<Object> values, Parameters parameterSettings) {
        List<Object> comps = new ArrayList<Object>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Object comp = values.get(i);
            ISelectable column = columns.get(i);
            if (processAutoIncrement()) {
                if (column.isAutoIncrement() && !(comp instanceof ISequenceVal)) {
                    comp = SequencePreProcessor.convertToSequence(column, comp, parameterSettings);
                }
            }

            if (comp instanceof IBindVal) {
                comps.add(((IBindVal) comp).assignment(parameterSettings));
            } else if (comp instanceof ISelectable) {
                comps.add(((ISelectable) comp).assignment(parameterSettings));
            } else {
                comps.add(comp);
            }
        }

        return comps;
    }

    public IFunction getNextSubqueryOnFilter() {
        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(this.getNode());
        if (func != null) {
            return (IFunction) func.copy();
        } else {
            return null;
        }
    }

    /**
     * 这个节点上执行哪些batch
     * 
     * @return
     */
    public List<Integer> getBatchIndexs() {
        return batchIndexs;
    }

    public void setBatchIndexs(List<Integer> batchIndexs) {
        this.batchIndexs = batchIndexs;
    }

    protected void copySelfTo(DMLNode to) {
        to.columns = this.columns;
        to.values = this.values;
        to.table = this.table;
        to.selectNode = this.selectNode;
        to.multiValues = this.multiValues;
        to.isMultiValues = this.isMultiValues;
        to.existSequenceVal = this.existSequenceVal;

        to.lowPriority = this.lowPriority;
        to.highPriority = this.highPriority;
        to.delayed = this.delayed;
        to.ignore = this.ignore;
        to.quick = this.quick;
        to.batchIndexs = this.batchIndexs;
        to.processAutoIncrement = this.processAutoIncrement;
    }

    protected void deepCopySelfTo(DMLNode to) {
        to.columns = OptimizerUtils.copySelectables(this.columns);
        if (this.values != null) {
            to.values = new ArrayList(this.values.size());
            for (Object value : this.values) {
                if (value instanceof ISelectable) {
                    to.values.add(((ISelectable) value).copy());
                } else if (value instanceof IBindVal) {
                    to.values.add(((IBindVal) value).copy());
                } else {
                    to.values.add(value);
                }
            }
        }

        to.setMultiValues(this.isMultiValues());
        if (this.multiValues != null) {
            List<List<Object>> multiValues = new ArrayList<List<Object>>(this.multiValues.size());
            for (List<Object> value : this.multiValues) {
                multiValues.add(OptimizerUtils.copyValues(value));
            }

            to.setMultiValues(multiValues);
        }

        to.table = this.table.deepCopy();
        if (this.getSelectNode() != null) {
            to.selectNode = this.selectNode.deepCopy();
        }
        to.existSequenceVal = this.existSequenceVal;

        to.lowPriority = this.lowPriority;
        to.highPriority = this.highPriority;
        to.delayed = this.delayed;
        to.ignore = this.ignore;
        to.quick = this.quick;
        to.batchIndexs = new ArrayList<Integer>(this.batchIndexs);
        to.processAutoIncrement = this.processAutoIncrement;
    }

    @Override
    public RT copySelf() {
        return copy();
    }

    @Override
    public String toString(int inden, int shareIndex) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + this.getClass().getSimpleName());
        appendField(sb, "columns", this.getColumns(), tabContent);
        if (isMultiValues()) {
            appendField(sb, "multiValues", this.getMultiValues(), tabContent);
        } else {
            appendField(sb, "values", this.getValues(), tabContent);
        }
        appendField(sb, "executeOn", this.getDataNode(shareIndex), tabContent);
        if (this.getNode() != null) {
            appendln(sb, tabContent + "query:");
            sb.append(this.getNode().toString(inden + 2));
        }

        if (this.getSelectNode() != null) {
            appendln(sb, tabContent + "select:");
            sb.append(this.getSelectNode().toString(inden + 2));
        }
        return sb.toString();
    }

    public QueryTreeNode getSelectNode() {
        return selectNode;
    }

    public void setSelectNode(QueryTreeNode selectNode) {
        this.selectNode = selectNode;
    }

    public void setProcessAutoIncrement(boolean processAutoIncrement) {
        this.processAutoIncrement = processAutoIncrement;
    }

}
