package com.taobao.tddl.optimizer.config.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 有schema文件的schemamanager实现
 * 
 * @since 5.0.0
 */
public class StaticSchemaManager extends AbstractLifecycle implements SchemaManager {

    private final static Logger       logger               = LoggerFactory.getLogger(StaticSchemaManager.class);
    public final static MessageFormat schemaNullError      = new MessageFormat("get schema null, appName is:{0}, unitName is:{1}, filePath is: {2}, dataId is: {3}");
    public final static MessageFormat TDDL5_SCHEMA_DATA_ID = new MessageFormat("com.taobao.tddl.v1_{0}_schema");
    public final static MessageFormat ANDOR_SCHEMA_DATA_ID = new MessageFormat("com.taobao.and_orV0.{0}_SCHEMA_DATAID");

    private ConfigDataHandler         schemaCdh;
    private String                    schemaFilePath       = null;
    private String                    appName              = null;
    private String                    unitName             = null;
    protected Map<String, TableMeta>  ss;

    private static TableMetaParser    parser               = new TableMetaParser();

    public StaticSchemaManager(String schemaFilePath, String appName, String unitName){
        super();
        this.schemaFilePath = schemaFilePath;
        this.appName = appName;
        this.unitName = unitName;
    }

    public StaticSchemaManager(){
    }

    private List<App>                 subApps          = null;
    private List<StaticSchemaManager> subSchemaManager = new ArrayList();

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    void loadDualTable() throws TddlException {
        ConfigDataHandler cdh = ConfigDataHandlerCity.getFileFactory(appName).getConfigDataHandler("DUAL_TABLE.xml",
            null);
        String data = cdh.getData();

        cdh.destroy();
        TableMetaParser parser = new TableMetaParser();

        TableMeta dualTable = parser.parse(data).get(0);

        this.putTable(dualTable.getTableName(), dualTable);
    }

    @Override
    protected void doInit() throws TddlException {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("StaticSchemaManager start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("schemaFile is: " + this.schemaFilePath);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("subApps is: " + this.subApps);

        super.doInit();
        ss = new ConcurrentHashMap<String, TableMeta>();
        loadDualTable();
        if (appName == null && schemaFilePath == null) {
            return;
        }

        if (this.subApps != null) {
            for (App subApp : subApps) {
                StaticSchemaManager subManager = new StaticSchemaManager(subApp.getSchemaFile(),
                    subApp.getAppName(),
                    this.unitName);

                try {
                    subManager.init();
                    this.subSchemaManager.add(subManager);
                } catch (Exception ex) {
                    logger.error("sub schema manager init error, sub app is: " + subApp, ex);
                }
            }
        }

        ConfigDataHandlerFactory factory = null;
        String dataId = null;

        // 优先从文件获取
        if (schemaFilePath == null) {
            dataId = TDDL5_SCHEMA_DATA_ID.format(new Object[] { appName });
            factory = ConfigDataHandlerCity.getFactory(appName, unitName);
        } else {
            factory = ConfigDataHandlerCity.getFileFactory(appName);
            dataId = schemaFilePath;
        }

        schemaCdh = factory.getConfigDataHandler(dataId, new SchemaConfigDataListener(this));

        String data = schemaCdh.getData();

        if (data == null || data.isEmpty()) {
            schemaCdh.destroy();
            // 尝试找一下andor的版本配置
            dataId = ANDOR_SCHEMA_DATA_ID.format(new Object[] { appName });
            schemaCdh = factory.getConfigDataHandler(dataId, new SchemaConfigDataListener(this));
            data = schemaCdh.getData();

        }

        if (data == null || data.isEmpty()) {
            logger.info(schemaNullError.format(new Object[] { appName, unitName, schemaFilePath, dataId }));
            return;
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            List<TableMeta> schemaList = parser.parse(sis);

            this.ss.clear();

            for (TableMeta table : schemaList) {
                this.putTable(table.getTableName(), table);
            }

            logger.info("table fetched:");
            logger.info(this.ss.keySet().toString());
            loadDualTable();
        } catch (Exception e) {
            logger.error("table parser error, schema file is:\n" + data, e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(sis);
        }

    }

    public class SchemaConfigDataListener implements ConfigDataListener {

        private StaticSchemaManager schemaManager;

        public SchemaConfigDataListener(StaticSchemaManager schemaManager){
            this.schemaManager = schemaManager;
        }

        @Override
        public void onDataRecieved(String dataId, String data) {
            if (data == null || data.isEmpty()) {
                logger.warn("schema is null, dataId is " + dataId);
                return;
            }

            InputStream sis = null;
            try {
                sis = new ByteArrayInputStream(data.getBytes());
                List<TableMeta> schemaList = parser.parse(sis);
                schemaManager.ss.clear();
                for (TableMeta table : schemaList) {
                    schemaManager.putTable(table.getTableName(), table);
                }

                logger.warn("table fetched:");
                logger.warn(schemaManager.ss.keySet().toString());
                loadDualTable();
            } catch (Exception e) {
                logger.error("table parser error, schema file is:" + data, e);
                throw new TddlNestableRuntimeException(e);
            } finally {
                IOUtils.closeQuietly(sis);
            }

        }

    }

    @Override
    protected void doDestroy() throws TddlException {
        super.doDestroy();
        ss.clear();
        if (schemaCdh != null) {
            schemaCdh.destroy();
        }
    }

    @Override
    public TableMeta getTable(String tableName) {
        TableMeta meta = ss.get((tableName));

        if (meta != null) {
            return meta;
        }

        if (this.subSchemaManager == null || subSchemaManager.isEmpty()) {
            return null;
        }

        for (StaticSchemaManager subManager : subSchemaManager) {
            meta = subManager.getTable(tableName);

            if (meta != null) return meta;
        }

        return null;
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        ss.put(tableName.toUpperCase(), tableMeta);
    }

    @Override
    public Collection<TableMeta> getAllTables() {
        return ss.values();
    }

    public static StaticSchemaManager parseSchema(String data) throws TddlException {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("schema is null");
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            return parseSchema(sis);
        } finally {
            IOUtils.closeQuietly(sis);
        }
    }

    public static StaticSchemaManager parseSchema(InputStream in) throws TddlException {
        if (in == null) {
            throw new IllegalArgumentException("in is null");
        }

        try {
            StaticSchemaManager schemaManager = new StaticSchemaManager();
            schemaManager.init();
            List<TableMeta> schemaList = parser.parse(in);
            for (TableMeta t : schemaList) {
                schemaManager.putTable(t.getTableName(), t);
            }
            return schemaManager;
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public void reload() throws TddlException {
        this.doDestroy();
        this.isInited = false;
        this.init();
    }
}
