package com.taobao.tddl.optimizer.parse.cobar;

import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.info.LastInsertId;
import com.alibaba.cobar.parser.ast.fragment.tableref.Dual;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.cobar.parser.ast.stmt.dal.DALShowStatement;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowIndex;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowSequences;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTables;
import com.alibaba.cobar.parser.ast.stmt.dal.TddlShow;
import com.alibaba.cobar.parser.ast.stmt.ddl.CreateSequence;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLCallStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLLoadStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.ast.stmt.reload.ReloadSchema;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dal.GetSequenceNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.cobar.CoronaSequenceProcessor.ProcessorResult;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlDeleteVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlInsertVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlLoadDataVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlReplaceVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlSelectVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlShowVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlUpdateVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MysqlTableVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.ReloadVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.SequenceVisitor;

/**
 * 基于cobar构造的parse结果
 */
@SuppressWarnings("deprecation")
public class CobarSqlAnalysisResult implements SqlAnalysisResult {

    private SqlType         sqlType;
    private SQLASTVisitor   visitor;
    private SQLStatement    statement;
    private boolean         hasVisited;
    private String          sql;
    private String          index;           // for eagleeye
    private String          parameterizedSql;
    private GetSequenceNode getSequence;

    public void build(String sql, String parameterizedSql, String index, SQLStatement statement) {
        if (sql != null) {
            this.sql = sql;
            this.parameterizedSql = parameterizedSql;
            this.statement = statement;
            this.index = index;
            if (statement instanceof DMLSelectStatement) {
                if (CoronaSequenceProcessor.isSequenceSql(sql)) {
                    ProcessorResult result = CoronaSequenceProcessor.process((DMLSelectStatement) statement);
                    if (result.flag) {
                        sqlType = SqlType.GET_SEQUENCE;
                        getSequence = new GetSequenceNode(result.name, result.count);
                        return;
                    }
                }

                if (isDualOrEmptyTable((DMLSelectStatement) statement)) {
                    List<Pair<Expression, String>> items = ((DMLSelectStatement) statement).getSelectExprList();
                    if (items != null && items.get(0).getKey() instanceof LastInsertId) {
                        sqlType = SqlType.SELECT_LAST_INSERT_ID;
                        visitor = new MySqlSelectVisitor(true);// 强制为dual表
                        return;
                    }
                }

                if (isEmptyTable((DMLSelectStatement) statement)) {
                    sqlType = SqlType.SELECT_WITHOUT_TABLE;
                } else {
                    sqlType = SqlType.SELECT;
                    visitor = new MySqlSelectVisitor();
                }
            } else if (statement instanceof DMLUpdateStatement) {
                sqlType = SqlType.UPDATE;
                visitor = new MySqlUpdateVisitor();
            } else if (statement instanceof DMLDeleteStatement) {
                sqlType = SqlType.DELETE;
                visitor = new MySqlDeleteVisitor();
            } else if (statement instanceof DMLInsertStatement) {
                sqlType = SqlType.INSERT;
                visitor = new MySqlInsertVisitor();
            } else if (statement instanceof DMLReplaceStatement) {
                sqlType = SqlType.REPLACE;
                visitor = new MySqlReplaceVisitor();
            } else if (statement instanceof CreateSequence) {
                sqlType = SqlType.CREATE_SEQUENCE;
                visitor = new SequenceVisitor();
            } else if (statement instanceof ShowSequences) {
                sqlType = SqlType.SHOW_SEQUENCES;
                visitor = new SequenceVisitor();
            } else if (statement instanceof TddlShow) {
                sqlType = SqlType.TDDL_SHOW;
                visitor = new MySqlShowVisitor();
            } else if (statement instanceof ShowTables) {
                sqlType = SqlType.TDDL_SHOW;
                visitor = new MySqlShowVisitor();
            } else if (statement instanceof DescTableStatement) {
                sqlType = SqlType.TDDL_SHOW;
                visitor = new MySqlShowVisitor();
            } else if (statement instanceof ShowCreate && ((ShowCreate) statement).getType() == ShowCreate.Type.TABLE) {
                sqlType = SqlType.TDDL_SHOW;
                visitor = new MySqlShowVisitor();
            } else if (statement instanceof ShowColumns || statement instanceof ShowIndex) {
                sqlType = SqlType.TDDL_SHOW;
                visitor = new MySqlShowVisitor();
            } else if (statement instanceof DMLCallStatement) {
                sqlType = SqlType.PROCEDURE;
            } else if (statement instanceof DALShowStatement) {
                sqlType = SqlType.SHOW;
            } else if (statement instanceof DMLLoadStatement) {
                sqlType = SqlType.LOAD;
                visitor = new MySqlLoadDataVisitor();
            } else if (statement instanceof DALSetStatement) {
                sqlType = SqlType.SET;

            } else if (statement instanceof ReloadSchema) {
                sqlType = SqlType.RELOAD;
                visitor = new ReloadVisitor();
            } else if (statement instanceof DDLStatement) {
                throw new IllegalArgumentException("tddl not support DDL statement:'" + sql + "'");
            } else {
                throw new IllegalArgumentException("not support  statement:'" + sql + "'");
            }
        }
    }

    public void visited() {
        if (hasVisited == false && statement != null && visitor != null) {
            statement.accept(visitor);
            hasVisited = true;
        }
    }

    @Override
    public SqlType getSqlType() {
        return sqlType;
    }

    @Override
    public QueryTreeNode getQueryTreeNode() {
        visited();
        return ((MySqlSelectVisitor) visitor).getTableNode();
    }

    @Override
    public UpdateNode getUpdateNode() {
        visited();
        return ((MySqlUpdateVisitor) visitor).getUpdateNode();
    }

    @Override
    public InsertNode getInsertNode() {
        visited();
        return ((MySqlInsertVisitor) visitor).getInsertNode();
    }

    @Override
    public PutNode getReplaceNode() {
        visited();
        return ((MySqlReplaceVisitor) visitor).getReplaceNode();
    }

    @Override
    public DeleteNode getDeleteNode() {
        visited();
        return ((MySqlDeleteVisitor) visitor).getDeleteNode();
    }

    @Override
    public ASTNode getAstNode() {
        if (sqlType == SqlType.SELECT || sqlType == SqlType.SELECT_LAST_INSERT_ID
            || sqlType == SqlType.SELECT_WITHOUT_TABLE) {
            return getQueryTreeNode();
        } else if (sqlType == SqlType.UPDATE) {
            return getUpdateNode();
        } else if (sqlType == SqlType.INSERT) {
            return getInsertNode();
        } else if (sqlType == SqlType.REPLACE) {
            return getReplaceNode();
        } else if (sqlType == SqlType.DELETE) {
            return getDeleteNode();
        } else if (sqlType == SqlType.CREATE_SEQUENCE) {
            visited();
            return ((SequenceVisitor) visitor).getCreateSequenceInsert();
        } else if (sqlType == SqlType.SHOW_SEQUENCES) {
            visited();
            return ((SequenceVisitor) visitor).getShowSequencesSelect();
        } else if (sqlType == SqlType.TDDL_SHOW) {
            visited();
            return ((MySqlShowVisitor) visitor).getNode();
        } else if (sqlType == SqlType.LOAD) {
            visited();
            ASTNode ast = ((MySqlLoadDataVisitor) visitor).getNode();
            ast.setSql(sql);
            return ast;
        } else if (sqlType == SqlType.GET_SEQUENCE) {
            return getSequence;
        } else if (sqlType == SqlType.RELOAD) {
            visited();
            return ((ReloadVisitor) visitor).getNode();
        }

        throw new NotSupportException(sqlType.toString());
    }

    public SQLStatement getStatement() {
        return statement;
    }

    @Override
    public String getSql() {
        return this.sql;
    }

    @Override
    public boolean isAstNode() {
        return !(sqlType == SqlType.PROCEDURE || sqlType == SqlType.SHOW || sqlType == SqlType.SELECT_WITHOUT_TABLE || sqlType == SqlType.SET);
    }

    @Override
    public Map<String, String> getTableNames() {
        MysqlTableVisitor visitor = new MysqlTableVisitor();
        statement.accept(visitor);
        return visitor.getTablesWithSchema();
    }

    public String getParameterizedSql() {
        return parameterizedSql;
    }

    private boolean isDualOrEmptyTable(DMLSelectStatement statement) {
        if (isEmptyTable(statement)) {
            return true;
        } else {
            TableReferences tables = statement.getTables();
            List<TableReference> trs = tables.getTableReferenceList();
            for (int i = 0; i < trs.size(); i++) {
                TableReference tr = trs.get(i);
                if (tr instanceof Dual) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isEmptyTable(DMLSelectStatement statement) {
        TableReferences tables = statement.getTables();
        if (tables == null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getIndex() {
        return index;
    }

}
