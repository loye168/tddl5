package com.taobao.tddl.optimizer.parse.cobar;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.util.Pair;

/**
 * 已经废弃，纯粹为兼容老的逻辑 <br/>
 * 路由处理：sequence<br>
 * sequence的SQL语句格式：SELECT CORONA_NEXT_VAL (FROM table) (WHERE CORONA_SEQ_COUNT
 * = count);
 * 
 * @author leiwen.zh
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
@Deprecated
public class CoronaSequenceProcessor {

    public static final String DEFAULT_SEQ_NAME     = "default";
    public static final String OLD_SEQ_NAME_IN_SQL  = "CORONA_NEXT_VAL";
    public static final String OLD_SEQ_WHERE_IN_SQL = "CORONA_SEQ_COUNT";
    public static final String SEQ_NAME_IN_SQL      = "DRDS_SEQ_VAL";
    public static final String SEQ_WHERE_IN_SQL     = "DRDS_SEQ_COUNT";

    public static class ProcessorResult {

        boolean flag  = false;
        String  name  = DEFAULT_SEQ_NAME;
        int     count = 1;
    }

    public static boolean isSequenceSql(String stmt) {
        String stmtInUpperCase = stmt.toUpperCase();
        if (stmtInUpperCase.contains(SEQ_NAME_IN_SQL) || stmtInUpperCase.contains(OLD_SEQ_NAME_IN_SQL)) {
            return true;
        }
        return false;
    }

    public static ProcessorResult process(DMLSelectStatement select) {
        ProcessorResult result = new ProcessorResult();
        List<Pair<Expression, String>> selectExprList = select.getSelectExprList();
        TableReferences tableReferences = select.getTables();
        // 仅支持 select CORONA_NEXT_VAL (from table),一个select域,一张表,表可以不带
        if (selectExprList != null && selectExprList.size() == 1) {
            Expression selectExpr = selectExprList.get(0).getKey();
            if (selectExpr instanceof Identifier) {
                Identifier selectIdentifier = (Identifier) selectExpr;
                if (SEQ_NAME_IN_SQL.equalsIgnoreCase(selectIdentifier.getIdTextUpUnescape())
                    || OLD_SEQ_NAME_IN_SQL.equalsIgnoreCase(selectIdentifier.getIdTextUpUnescape())) {
                    result.flag = true;

                    if (tableReferences != null) {
                        List<TableReference> tables = tableReferences.getTableReferenceList();
                        if (tables != null) {
                            if (tables.size() == 1) {
                                String tableName = ((TableRefFactor) tables.get(0)).getTable().getIdText();
                                result.name = tableName;
                            }
                        }
                    }

                    // 一次需要拿多少个值,根据where条件判断,where COUNT =
                    // 100,如果没有where条件,一次获取一个值
                    Expression where = select.getWhere();
                    if (where instanceof ComparisionEqualsExpression) {
                        ComparisionEqualsExpression equalExp = (ComparisionEqualsExpression) where;
                        Expression left = equalExp.getLeftOprand();
                        Expression right = equalExp.getRightOprand();
                        if (left instanceof Identifier && right instanceof LiteralNumber) {
                            Identifier whereKey = (Identifier) left;
                            LiteralNumber whereValue = (LiteralNumber) right;
                            if (whereKey.getIdText().toUpperCase().equalsIgnoreCase(SEQ_WHERE_IN_SQL)
                                || whereKey.getIdText().toUpperCase().equalsIgnoreCase(OLD_SEQ_WHERE_IN_SQL)) {
                                result.count = whereValue.getNumber().intValue();
                            }
                        }
                    }
                }
            }
        }

        return result;
    }

}
