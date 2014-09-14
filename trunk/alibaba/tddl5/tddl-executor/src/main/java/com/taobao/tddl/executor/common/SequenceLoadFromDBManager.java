package com.taobao.tddl.executor.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.impl.DefaultSequence;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.client.sequence.impl.GroupSequenceDao;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * 直接读取seqeunce表，生成seqeunce配置
 * 
 * @author mengshi.sunmengshi 2014年5月7日 下午2:08:15
 * @since 5.1.0
 */
public class SequenceLoadFromDBManager extends AbstractSequenceManager {

    private final static Logger            logger                 = LoggerFactory.getLogger(SequenceLoadFromDBManager.class);
    public final static MessageFormat      TDDL5_SEQUENCE_DATA_ID = new MessageFormat("com.taobao.tddl.sequence.{0}");
    private LoadingCache<String, Sequence> cache                  = null;
    private String                         appName                = null;
    private String                         unitName               = null;
    private OptimizerRule                  rule;
    private Sequence                       NULL_OBJ               = new DefaultSequence();
    private TGroupDataSource               ds;
    private String                         seqTable;
    private String                         groupKey;
    private GroupSequenceDao               sd;

    public SequenceLoadFromDBManager(String appName, String unitName, OptimizerRule rule){
        this.appName = appName;
        this.unitName = unitName;
        this.rule = rule;
    }

    public Sequence getSequence0(String seqName) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement("select name from " + this.seqTable + " where name=?");
            stmt.setString(1, seqName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                return buildSequence(name);
            }

            if (seqName.startsWith(ISequenceManager.AUTO_SEQ_PREFIX)) {
                // 如果是数据库自增id,自动创建一个sequence
                return buildSequence(seqName);
            }
            return NULL_OBJ;
        } catch (SQLException ex) {

            // not exists sequence table on default db index
            if (ex.getMessage() != null && ex.getMessage().contains("doesn't exist")) {
                throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE_TABLE_ON_DEFAULT_DB);
            } else if (ex.getMessage() != null && ex.getMessage().contains("Unknown column")) {
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_TABLE_META);
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER_WHEN_BUILD_SEQUENCE, ex, ex.getMessage());

        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    protected Sequence getSequence(String name) {
        try {
            Sequence seq = cache.get(name);
            if (seq == NULL_OBJ) {
                return null;
            } else {
                return seq;
            }
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    public void doInit() {
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, Sequence>() {

            @Override
            public Sequence load(String tableName) throws Exception {
                return getSequence0(tableName);
            }
        });

        this.seqTable = "sequence";
        TargetDB targetDB = rule.shardAny(seqTable);
        this.groupKey = targetDB.getDbIndex();
        if (groupKey == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE_DEFAULT_DB);
        }

        seqTable = targetDB.getTableNames().iterator().next();
        this.ds = null;
        try {
            ds = new TGroupDataSource();
            ds.setAppName(appName);
            ds.setUnitName(unitName);
            ds.setDbGroupKey(groupKey);
            ds.init();
            this.sd = new GroupSequenceDao();
            List groupKeys = new ArrayList();
            groupKeys.add(groupKey);
            sd.setDbGroupKeys(groupKeys);
            sd.setAppName(this.appName);
            sd.setUnitName(this.unitName);
            sd.setAdjust(true);
            sd.setDscount(1);

            sd.init();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INIT_SEQUENCE_FROM_DB, ex, ex.getMessage());
        }

    }

    @Override
    protected void doDestroy() {
        try {
            ds.destroy();
        } catch (TddlException e) {
            logger.error(e);
        }

        if (cache != null) {
            cache.cleanUp();
        }
    }

    private Sequence buildSequence(String name) throws SQLException {
        GroupSequence seq = new GroupSequence();
        seq.setName(name);
        seq.setSequenceDao(sd);
        seq.init();
        logger.info("seqeunce init:");
        logger.info(seq.toString());
        return seq;
    }
}
