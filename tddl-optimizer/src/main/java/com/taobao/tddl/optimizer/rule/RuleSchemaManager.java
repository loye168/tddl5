package com.taobao.tddl.optimizer.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.IndexType;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.rule.model.TargetDB;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 基于Rule获取到物理的group进行查找
 * 
 * @since 5.0.0
 */
public class RuleSchemaManager extends AbstractLifecycle implements SchemaManager {

    private static final Logger                    logger          = LoggerFactory.getLogger(RuleSchemaManager.class);
    private OptimizerRule                          rule;
    private final Matrix                           matrix;
    private StaticSchemaManager                    local;
    private boolean                                useCache        = true;
    private LoadingCache<Group, RepoSchemaManager> repos           = null;
    private LoadingCache<String, TableMeta>        cache           = null;

    /**
     * default cache expire time, 30000ms
     */
    private long                                   cacheExpireTime = TddlConstants.DEFAULT_TABLE_META_EXPIRE_TIME;

    public RuleSchemaManager(OptimizerRule rule, Matrix matrix){
        this(rule, matrix, null);
    }

    public RuleSchemaManager(OptimizerRule rule, Matrix matrix, Long cacheExpireTime){
        this.rule = rule;
        this.matrix = matrix;

        if (cacheExpireTime != null && cacheExpireTime != 0) {
            this.cacheExpireTime = cacheExpireTime;
        }

    }

    @Override
    protected void doInit() throws TddlException {
        super.doInit();

        if (local != null) {
            local.init();
        }
        repos = CacheBuilder.newBuilder().build(new CacheLoader<Group, RepoSchemaManager>() {

            @Override
            public RepoSchemaManager load(Group group) throws Exception {
                RepoSchemaManager repo = new RepoSchemaManager();
                repo.setGroup(group);
                repo.setLocal(local);
                repo.setRule(rule);
                repo.init();
                return repo;
            }
        });

        cache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(cacheExpireTime, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<String, TableMeta>() {

                @Override
                public TableMeta load(String tableName) throws Exception {
                    return getTable0(tableName);

                }
            });
    }

    @Override
    protected void doDestroy() throws TddlException {
        super.doDestroy();

        for (RepoSchemaManager repo : repos.asMap().values()) {
            repo.destroy();
        }

        if (cache != null) {
            cache.cleanUp();
        }
    }

    private TableMeta getTable0(String tableName) {
        TargetDB targetDB = rule.shardAny(tableName);
        TableMeta ts = null;
        if (targetDB.getDbIndex() == null) {
            // 没有对应的规则，也没有default group，则可能是一个不存在的表
            // 尝试找一下local
            ts = local.getTable(tableName);
        } else {
            Group group = matrix.getGroup(targetDB.getDbIndex()); // 先找到group

            if (group == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "not found groupName : " + targetDB.getDbIndex());
            }
            try {
                ts = repos.get(group).getTable(tableName, targetDB.getTableNames().iterator().next());
            } catch (ExecutionException e) {
                logger.error(ErrorCode.ERR_CONFIG.getMessage("not found table : " + tableName), e);
            }
        }

        return ts;
    }

    @Override
    public TableMeta getTable(String tableName) {
        // if (tableName.equals(DUAL)) {
        // return buildDualTable();
        // }

        TableMeta meta = null;
        if (local != null) {// 本地如果开启了，先找本地
            meta = local.getTable(tableName);
        }

        if (meta != null) {
            return meta;
        }

        if (useCache) {
            try {
                meta = cache.get(tableName);
            } catch (Throwable e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, "not found table : " + tableName);
            }
        } else {
            meta = this.getTable0(tableName);
        }

        return meta;
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        if (local != null) {
            local.putTable(tableName, tableMeta);
        } else if (useCache) {
            cache.put(tableName, tableMeta);
        }
    }

    @Override
    public Collection<TableMeta> getAllTables() {
        List<TableMeta> metas = new ArrayList();
        if (local != null) {
            metas.addAll(local.getAllTables());
        }

        if (cache != null) {
            metas.addAll(cache.asMap().values());
        }

        return metas;
    }

    protected TableMeta buildDualTable() {
        IndexMeta index = new IndexMeta(SchemaManager.DUAL,
            new ArrayList<ColumnMeta>(),
            new ArrayList<ColumnMeta>(),
            IndexType.NONE,
            Relationship.NONE,
            false,
            true);

        return new TableMeta(DUAL, new ArrayList<ColumnMeta>(), index, new ArrayList<IndexMeta>());
    }

    public void setRule(OptimizerRule rule) {
        this.rule = rule;
    }

    public void setLocal(StaticSchemaManager local) {
        this.local = local;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    @Override
    public void reload() throws TddlException {
        this.cache.invalidateAll();
    }

}
