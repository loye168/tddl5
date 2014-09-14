package com.taobao.tddl.optimizer;

import java.util.Map;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exception.OptimizerException;

/**
 * 优化器执行接口
 * 
 * @since 5.0.0
 */
public interface Optimizer extends Lifecycle {

    /**
     * 基于语法树进行优化
     */
    ASTNode optimizeAst(ASTNode node, Parameters parameterSettings, Map<String, Object> extraCmd)
                                                                                                 throws OptimizerException;

    /**
     * 基于sql进行语法树构建+优化 , cache变量可控制优化的语法树是否会被缓存
     */
    Object optimizeAstOrHint(String sql, Parameters parameterSettings, boolean cached, Map<String, Object> extraCmd)
                                                                                                                    throws OptimizerException;

    /**
     * 设置对应子查询的执行结果,并返回下一个subquery function
     */
    IFunction assignmentSubquery(ASTNode node, Map<Long, Object> subquerySettings, Map<String, Object> extraCmd)
                                                                                                                throws OptimizerException;

    /**
     * 将语法树生成对应执行计划, 注意：需要先调用optimizeAst进行语法树优化
     */
    IDataNodeExecutor optimizePlan(ASTNode node, Parameters parameterSettings, Map<String, Object> extraCmd)
                                                                                                            throws OptimizerException;

}
