package com.taobao.tddl.atom.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.atom.TAtomDbStatusEnum;
import com.taobao.tddl.atom.TAtomDbTypeEnum;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.atom.utils.ConnRestrictEntry;
import com.taobao.tddl.common.model.DataSourceType;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * TAtom数据源全局和应用配置的DO
 * 
 * @author qihao
 * @author shenxun
 */
public class TAtomDsConfDO implements Cloneable {

    /**
     * 默认初始化的线程池连接数量
     */
    public static final int         defaultInitPoolSize  = 0;

    /**
     * 默认初始化的defaultMaxWait druid专用，目前是和jboss的blockingTimeout是同一个配置。运维人员请注意。
     */
    public static final int         defaultMaxWait       = 5000;

    private String                  ip;

    private String                  port;

    private String                  dbName;

    private String                  userName;

    private String                  passwd;

    private String                  driverClass;

    private String                  sorterClass;

    private int                     preparedStatementCacheSize;

    private int                     initPoolSize         = defaultInitPoolSize;

    private int                     minPoolSize;

    private int                     maxPoolSize;

    private int                     blockingTimeout      = defaultMaxWait;

    private long                    idleTimeout;

    // private String dbType;

    private String                  oracleConType        = TAtomConstants.DEFAULT_ORACLE_CON_TYPE;

    private TAtomDbTypeEnum         dbTypeEnum;

    private TAtomDbStatusEnum       dbStautsEnum;

    private String                  dbStatus;

    private Map<String, String>     connectionProperties = new HashMap<String, String>();

    /**
     * 写 次数限制
     */
    private int                     writeRestrictTimes;

    /**
     * 读 次数限制
     */
    private int                     readRestrictTimes;

    /**
     * 统计时间片
     */
    private int                     timeSliceInMillis;

    /**
     * 线程技术count限制
     */
    private int                     threadCountRestrict;

    /**
     * 允许并发读的最大个数，0为不限制
     */
    private int                     maxConcurrentReadRestrict;

    /**
     * 允许并发写的最大个数，0为不限制
     */
    private int                     maxConcurrentWriteRestrict;

    private volatile boolean        isSingleInGroup;

    /**
     * 应用连接限制: 限制某个应用键值的并发连接数。
     */
    private List<ConnRestrictEntry> connRestrictEntries;

    private String                  connectionInitSql;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getDriverClass() {
        if (TStringUtil.isBlank(driverClass) && null != this.dbTypeEnum) {
            return this.dbTypeEnum.getDriverClass();
        }
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getSorterClass() {
        if (TStringUtil.isBlank(sorterClass) && null != this.dbTypeEnum) {
            return this.dbTypeEnum.getSorterClass();
        }
        return sorterClass;
    }

    public void setSorterClass(String sorterClass) {
        this.sorterClass = sorterClass;
    }

    public int getPreparedStatementCacheSize() {
        return preparedStatementCacheSize;
    }

    public void setPreparedStatementCacheSize(int preparedStatementCacheSize) {
        this.preparedStatementCacheSize = preparedStatementCacheSize;
    }

    public int getInitPoolSize() {
        return initPoolSize;
    }

    public void setInitPoolSize(int initPoolSize) {
        this.initPoolSize = initPoolSize;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getBlockingTimeout() {
        return blockingTimeout;
    }

    public void setBlockingTimeout(int blockingTimeout) {
        this.blockingTimeout = blockingTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Map<String, String> getConnectionProperties() {
        return connectionProperties;
    }

    public String getDbType() {
        return dbTypeEnum.name().toLowerCase();
    }

    public void setDbType(String dbType) {
        this.dbTypeEnum = TAtomDbTypeEnum.getAtomDbTypeEnum(dbType, DataSourceType.DruidDataSource);
    }

    public String getDbStatus() {
        return dbStatus;
    }

    public void setDbStatus(String dbStatus) {
        this.dbStatus = dbStatus;
        if (TStringUtil.isNotBlank(dbStatus)) {
            this.dbStautsEnum = TAtomDbStatusEnum.getAtomDbStatusEnumByType(dbStatus);
        }
    }

    public TAtomDbStatusEnum getDbStautsEnum() {
        return dbStautsEnum;
    }

    public TAtomDbTypeEnum getDbTypeEnum() {
        return dbTypeEnum;
    }

    public void setConnectionProperties(Map<String, String> connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public String getOracleConType() {
        return oracleConType;
    }

    public void setOracleConType(String oracleConType) {
        this.oracleConType = oracleConType;
    }

    public int getWriteRestrictTimes() {
        return writeRestrictTimes;
    }

    public void setWriteRestrictTimes(int writeRestrictTimes) {
        this.writeRestrictTimes = writeRestrictTimes;
    }

    public int getReadRestrictTimes() {
        return readRestrictTimes;
    }

    public void setReadRestrictTimes(int readRestrictTimes) {
        this.readRestrictTimes = readRestrictTimes;
    }

    public int getThreadCountRestrict() {
        return threadCountRestrict;
    }

    public void setThreadCountRestrict(int threadCountRestrict) {
        this.threadCountRestrict = threadCountRestrict;
    }

    public int getTimeSliceInMillis() {
        return timeSliceInMillis;
    }

    public void setTimeSliceInMillis(int timeSliceInMillis) {
        this.timeSliceInMillis = timeSliceInMillis;
    }

    public int getMaxConcurrentReadRestrict() {
        return maxConcurrentReadRestrict;
    }

    public void setMaxConcurrentReadRestrict(int maxConcurrentReadRestrict) {
        this.maxConcurrentReadRestrict = maxConcurrentReadRestrict;
    }

    public int getMaxConcurrentWriteRestrict() {
        return maxConcurrentWriteRestrict;
    }

    public void setMaxConcurrentWriteRestrict(int maxConcurrentWriteRestrict) {
        this.maxConcurrentWriteRestrict = maxConcurrentWriteRestrict;
    }

    public List<ConnRestrictEntry> getConnRestrictEntries() {
        return connRestrictEntries;
    }

    public void setConnRestrictEntries(List<ConnRestrictEntry> connRestrictEntries) {
        this.connRestrictEntries = connRestrictEntries;
    }

    public boolean isSingleInGroup() {
        return isSingleInGroup;
    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.isSingleInGroup = isSingleInGroup;
    }

    public String getConnectionInitSql() {
        return connectionInitSql;
    }

    public void setConnectionInitSql(String connectionInitSql) {
        this.connectionInitSql = connectionInitSql;
    }

    public TAtomDsConfDO clone() {
        TAtomDsConfDO tAtomDsConfDO = null;
        try {
            tAtomDsConfDO = (TAtomDsConfDO) super.clone();
        } catch (CloneNotSupportedException e) {
        }
        return tAtomDsConfDO;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
