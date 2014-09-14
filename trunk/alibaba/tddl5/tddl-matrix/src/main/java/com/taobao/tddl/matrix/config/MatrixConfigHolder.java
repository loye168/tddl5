package com.taobao.tddl.matrix.config;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Group.GroupType;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.config.ConfigDataMode;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;
import com.taobao.tddl.config.impl.holder.ConfigHolderFactory;
import com.taobao.tddl.executor.TopologyExecutor;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.SequenceLoadFromDBManager;
import com.taobao.tddl.executor.common.SequenceManager;
import com.taobao.tddl.executor.common.TopologyHandler;
import com.taobao.tddl.optimizer.Optimizer;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.LocalStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.StatManager;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.rule.RuleIndexManager;
import com.taobao.tddl.optimizer.rule.RuleSchemaManager;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;
import com.taobao.tddl.rule.TddlRule;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 依赖的组件
 * 
 * @since 5.0.0
 */
public class MatrixConfigHolder extends AbstractConfigDataHolder {

    private final static Logger logger                   = LoggerFactory.getLogger(MatrixConfigHolder.class);
    private static final String GROUP_CONFIG_HOLDER_NAME = "com.taobao.tddl.group.config.GroupConfigHolder";
    private String              appName;
    private String              unitName;
    private String              ruleFilePath;
    private boolean             dynamicRule;
    private boolean             sharding                 = true;
    private String              schemaFilePath;
    private String              topologyFilePath;
    private OptimizerRule       optimizerRule;
    private TopologyHandler     topologyHandler;
    private TopologyExecutor    topologyExecutor;
    private ISequenceManager    seqManager               = null;
    private SchemaManager       schemaManager;
    private IndexManager        indexManger;
    private Optimizer           optimizer;
    private SqlParseManager     sqlParseManager;
    private OptimizerContext    optimizerContext;
    private ExecutorContext     executorContext;
    private StatManager         statManager;
    private Matrix              matrix;
    private Map<String, Object> connectionProperties;
    private final boolean       createGroupExecutor      = true;
    private List<App>           subApps;
    private String              sequenceFile;

    @Override
    public void doInit() throws TddlException {
        loadDelegateExtension();

        ExecutorContext executorContext = new ExecutorContext();
        this.executorContext = executorContext;
        ExecutorContext.setContext(executorContext);

        OptimizerContext oc = new OptimizerContext();
        this.optimizerContext = oc;
        OptimizerContext.setContext(oc);

        topologyInit();
        ruleInit();
        schemaInit();
        optimizerInit();

        executorContext.setTopologyHandler(topologyHandler);
        executorContext.setTopologyExecutor(topologyExecutor);

        oc.setIndexManager(this.indexManger);
        oc.setMatrix(topologyHandler.getMatrix());
        oc.setSchemaManager(schemaManager);
        oc.setRule(optimizerRule);
        oc.setOptimizer(this.optimizer);
        oc.setSqlParseManager(this.sqlParseManager);
        oc.setStatManager(this.statManager);

        // 添加matrix参数
        addDatas(matrix.getProperties());
        initSonHolder();
        // 将自己做为config holder
        try {
            ConfigHolderFactory.addConfigDataHolder(appName, this);
            if (createGroupExecutor) {
                initGroups();
            }

            sequenceInit();
            executorContext.setSeqeunceManager(seqManager);
        } finally {
            ConfigHolderFactory.removeConfigHoder(appName);
        }
    }

    private void sequenceInit() throws TddlException {
        SequenceLoadFromDBManager subManager = new SequenceLoadFromDBManager(appName, unitName, optimizerRule);
        this.seqManager = new SequenceManager(appName, unitName, sequenceFile, subManager);

        if (ConfigDataMode.getMode().isMock()) {
            this.seqManager.init();
        }
    }

    @Override
    protected void doDestroy() throws TddlException {
        schemaManager.destroy();
        optimizerRule.destroy();
        optimizer.destroy();
        statManager.destroy();
        topologyHandler.destroy();
        topologyExecutor.destroy();
    }

    public void topologyInit() throws TddlException {
        topologyHandler = new TopologyHandler(appName, unitName, topologyFilePath, this.connectionProperties);
        topologyHandler.setSubApps(this.subApps);
        topologyHandler.init();
        executorContext.setRepositoryHolder(topologyHandler.getRepositoryHolder());

        topologyExecutor = new TopologyExecutor();
        topologyExecutor.init();
        matrix = topologyHandler.getMatrix();
    }

    public void ruleInit() throws TddlException {
        TddlRule rule = new TddlRule();
        rule.setAppName(appName);
        rule.setSubApps(this.subApps);
        rule.setUnitName(this.unitName);
        rule.setAppRuleFile(ruleFilePath);
        rule.setAllowEmptyRule(!sharding);
        if (matrix.getGroups() != null) {
            String singleDbIndex = matrix.getGroups().get(0).getName();
            rule.setDefaultDbIndex(singleDbIndex);
        }

        rule.init();
        optimizerRule = new OptimizerRule(rule);
        optimizerRule.init();
    }

    public void schemaInit() throws TddlException {
        RuleSchemaManager ruleSchemaManager = new RuleSchemaManager(optimizerRule,
            topologyHandler.getMatrix(),
            GeneralUtil.getExtraCmdLong(this.connectionProperties,
                ConnectionProperties.TABLE_META_CACHE_EXPIRE_TIME,
                TddlConstants.DEFAULT_TABLE_META_EXPIRE_TIME));
        StaticSchemaManager staticSchemaManager = new StaticSchemaManager(schemaFilePath, appName, unitName);

        staticSchemaManager.setSubApps(subApps);

        ruleSchemaManager.setLocal(staticSchemaManager);

        this.schemaManager = ruleSchemaManager;
        schemaManager.init();

        IndexManager indexManager = new RuleIndexManager(ruleSchemaManager);
        indexManager.init();
        this.indexManger = indexManager;

    }

    public void optimizerInit() throws TddlException {
        CostBasedOptimizer optimizer = new CostBasedOptimizer(optimizerRule);
        optimizer.setExpireTime(GeneralUtil.getExtraCmdLong(this.connectionProperties,
            ConnectionProperties.OPTIMIZER_CACHE_EXPIRE_TIME,
            TddlConstants.DEFAULT_OPTIMIZER_EXPIRE_TIME));
        optimizer.setCacheSize(GeneralUtil.getExtraCmdLong(this.connectionProperties,
            ConnectionProperties.OPTIMIZER_CACHE_SIZE,
            TddlConstants.DEFAULT_OPTIMIZER_CACHE_SIZE));
        optimizer.init();

        this.optimizer = optimizer;
        this.sqlParseManager = optimizer.getSqlParseManager();

        // RuleStatManager statManager = new RuleStatManager(optimizerRule,
        // topologyHandler.getMatrix());
        LocalStatManager statManager = new LocalStatManager();
        statManager.init();

        this.statManager = statManager;
    }

    protected void initSonHolder() throws TddlException {
        Class sonHolderClass = null;
        try {
            sonHolderClass = Class.forName(GROUP_CONFIG_HOLDER_NAME);
        } catch (ClassNotFoundException e1) {
            // ignore , 可能不需要使用group层
            return;
        }

        try {
            List<Group> groups = new ArrayList<Group>();
            for (Group group : matrix.getGroups()) {
                GroupType type = group.getType();
                if (type.isMysql() || type.isOracle()) {
                    groups.add(group);
                }
            }
            Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
            sonConfigDataHolder = (AbstractConfigDataHolder) constructor.newInstance(this.appName,
                groups,
                this.unitName);
            sonConfigDataHolder.init();
            delegateDataHolder.setSonConfigDataHolder(sonConfigDataHolder);// 传递给deletegate，由它进行son传递
        } catch (Exception e) {
            // batch出错，走到老逻辑，单条获取，不直接抛异常
            logger.info(e);
        }
    }

    protected void initGroups() throws TddlException {
        for (Group group : matrix.getGroups()) {
            topologyHandler.createOne(group);
        }
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getRuleFilePath() {
        return ruleFilePath;
    }

    public void setRuleFilePath(String ruleFilePath) {
        this.ruleFilePath = ruleFilePath;
    }

    public String getSchemaFilePath() {
        return schemaFilePath;
    }

    public void setSchemaFilePath(String schemaFilePath) {
        this.schemaFilePath = schemaFilePath;
    }

    public String getTopologyFilePath() {
        return topologyFilePath;
    }

    public void setTopologyFilePath(String topologyFilePath) {
        this.topologyFilePath = topologyFilePath;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public ExecutorContext getExecutorContext() {
        return this.executorContext;
    }

    public OptimizerContext getOptimizerContext() {
        return this.optimizerContext;
    }

    public Map<String, Object> getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Map<String, Object> connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public boolean isDynamicRule() {
        return dynamicRule;
    }

    public void setDynamicRule(boolean dynamicRule) {
        this.dynamicRule = dynamicRule;
    }

    public void setSharding(boolean sharding) {
        this.sharding = sharding;
    }

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    public String getSequenceFile() {
        return sequenceFile;
    }

    public void setSequenceFile(String sequenceFile) {
        this.sequenceFile = sequenceFile;
    }

}
