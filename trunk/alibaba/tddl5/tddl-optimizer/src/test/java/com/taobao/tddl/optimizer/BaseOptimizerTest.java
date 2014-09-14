package com.taobao.tddl.optimizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.LocalStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.StatManager;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.rule.RuleIndexManager;
import com.taobao.tddl.rule.TddlRule;

@Ignore
public class BaseOptimizerTest {

    protected static final String       APPNAME               = "tddl";
    protected static final String       table_file            = "config/test_table.xml";
    protected static final String       matrix_file           = "config/test_matrix.xml";
    protected static final String       rule_file             = "config/test_rule.xml";
    protected static final String       table_stat_file       = "config/table_stat.xml";
    protected static final String       table_index_stat_file = "config/kvIndex_stat.xml";

    protected static SqlParseManager    parser                = new CobarSqlParseManager();
    protected static OptimizerRule      rule;
    protected static RepoSchemaManager  schemaManager;
    protected static CostBasedOptimizer optimizer;
    protected static StatManager        statManager;

    @BeforeClass
    public static void initial() throws TddlException {
        parser.init();

        OptimizerContext context = new OptimizerContext();
        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppRuleFile("classpath:" + rule_file);
        tddlRule.setAppName(APPNAME);
        tddlRule.init();

        rule = new OptimizerRule(tddlRule);

        StaticSchemaManager localSchemaManager = StaticSchemaManager.parseSchema(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(table_file));

        Matrix matrix = MatrixParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(matrix_file));

        schemaManager = new RepoSchemaManager();
        schemaManager.setLocal(localSchemaManager);
        schemaManager.setGroup(matrix.getGroup("andor_group_0"));
        schemaManager.init();

        statManager = LocalStatManager.parseConfig(GeneralUtil.getInputStream(table_stat_file),
            GeneralUtil.getInputStream(table_index_stat_file));

        // statManager = new RepoStatManager();
        // statManager.setLocal(local);
        // statManager.setUseCache(true);
        // statManager.setGroup(matrix.getGroup("andor_group_0"));
        // statManager.init();

        context.setMatrix(matrix);
        context.setRule(rule);
        context.setSchemaManager(schemaManager);
        context.setStatManager(statManager);
        context.setIndexManager(new RuleIndexManager(schemaManager));

        OptimizerContext.setContext(context);

        optimizer = new CostBasedOptimizer(rule);
        optimizer.setSqlParseManager(parser);
        optimizer.init();
    }

    @AfterClass
    public static void tearDown() throws TddlException {
        schemaManager.destroy();
        statManager.destroy();
        parser.destroy();
        optimizer.destroy();
    }

    protected Parameters convert(List<Object> args) {
        Map<Integer, ParameterContext> map = new HashMap<Integer, ParameterContext>(args.size());
        int index = 1;
        for (Object obj : args) {
            ParameterContext context = new ParameterContext(ParameterMethod.setObject1, new Object[] { index, obj });
            map.put(index, context);
            index++;
        }
        return new Parameters(map, false);
    }

    protected Parameters convert(Object[] args) {
        return convert(Arrays.asList(args));
    }

}
