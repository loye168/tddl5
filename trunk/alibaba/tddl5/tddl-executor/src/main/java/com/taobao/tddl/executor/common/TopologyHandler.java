package com.taobao.tddl.executor.common;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Group.GroupType;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;

/**
 * group以及其对应的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.0.0
 */
public class TopologyHandler extends AbstractLifecycle {

    public final static Logger                               logger              = LoggerFactory.getLogger(TopologyHandler.class);
    public final static String                               xmlHead             = "<matrix xmlns=\"https://github.com/tddl/tddl/schema/matrix\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"https://github.com/tddl/tddl/schema/matrix https://raw.github.com/tddl/tddl/master/tddl-common/src/main/resources/META-INF/matrix.xsd\">";
    public final static MessageFormat                        topologyNullError   = new MessageFormat("get topology info error, appName is:{0}, unitName is:{1}, filePath is: {2}, dataId is: {3}");
    public final static MessageFormat                        TOPOLOGY            = new MessageFormat("com.taobao.tddl.v1_{0}_topology");

    /**
     * 为了和andor的配置兼容
     */
    public final static MessageFormat                        ANDOR_TOPOLOGY      = new MessageFormat("com.taobao.and_orV0.{0}_MACHINE_TAPOLOGY");
    public final static MessageFormat                        GROUP_TOPOLOGY      = new MessageFormat("com.taobao.tddl.v1_{0}_dbgroups");
    private final Map<String/* group key */, IGroupExecutor> executorMap         = new HashMap<String, IGroupExecutor>();
    private String                                           appName;
    private String                                           unitName;
    private String                                           topologyFilePath;
    private ConfigDataHandler                                topologyFileHandler = null;
    private Cache<String, ConfigDataHandler>                 cdhs                = CacheBuilder.newBuilder().build();
    private Matrix                                           matrix;
    private Map                                              cp;
    private TopologyListener                                 topologyListener;
    private List<App>                                        subApps             = null;
    private List<TopologyHandler>                            subTopologyHandlers = new ArrayList();
    private RepositoryHolder                                 repositoryHolder    = new RepositoryHolder();

    public TopologyHandler(String appName, String unitName, String topologyFilePath){
        this.appName = appName;
        this.unitName = unitName;
        this.topologyFilePath = topologyFilePath;
        this.topologyListener = new TopologyListener(this);
        this.repositoryHolder = new RepositoryHolder();
    }

    public TopologyHandler(String appName, String unitName, String topologyFilePath, Map cp){
        this(appName, unitName, topologyFilePath);
        this.cp = cp;
    }

    @Override
    protected void doInit() {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TopologyHandler start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("topologyFile is: " + this.topologyFilePath);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("subApps is: " + this.subApps);
        try {
            this.matrix = initMatrix(appName, topologyFilePath);
        } catch (Exception ex) {
            logger.error("matrix topology init error,file is: " + this.getTopologyFilePath() + ", appname is: "
                         + this.getAppName(),
                ex);
            throw new TddlNestableRuntimeException(ex);
        }

    }

    private Matrix initMatrix(final String appName, String topologyFilePath) {
        String data = null;
        if (topologyFilePath != null) {
            topologyFileHandler = ConfigDataHandlerCity.getFileFactory(appName).getConfigDataHandler(topologyFilePath,
                new TopologyListener(this));
            data = topologyFileHandler.getData();
        } else {
            final String dataId = TOPOLOGY.format(new Object[] { appName });
            ConfigDataHandler cdh = null;
            try {
                cdh = cdhs.get(dataId, new Callable<ConfigDataHandler>() {

                    @Override
                    public ConfigDataHandler call() throws Exception {
                        return ConfigDataHandlerCity.getFactory(appName, unitName).getConfigDataHandler(dataId,
                            topologyListener);
                    }
                });
            } catch (ExecutionException e) {
                throw new TddlNestableRuntimeException(e);
            }

            data = cdh.getData();

            /**
             * tddl的topology找不到，试试andor的
             */
            if (data == null) {
                final String andorDataId = ANDOR_TOPOLOGY.format(new Object[] { appName });
                try {
                    cdh = cdhs.get(andorDataId, new Callable<ConfigDataHandler>() {

                        @Override
                        public ConfigDataHandler call() throws Exception {
                            return ConfigDataHandlerCity.getFactory(appName, unitName)
                                .getConfigDataHandler(andorDataId, topologyListener);
                        }
                    });
                } catch (ExecutionException e) {
                    throw new TddlNestableRuntimeException(e);
                }

                data = cdh.getData(ConfigDataHandler.GET_DATA_TIMEOUT, ConfigDataHandler.FIRST_SERVER_STRATEGY);

            }
        }

        if (data == null) {
            data = generateTopologyXML(appName, unitName);
        }

        if (data == null) {
            String dataId = TOPOLOGY.format(new Object[] { appName });
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG_MISS_TOPOLOGY, dataId);
        }

        Matrix matrix = MatrixParser.parse(data);

        Group dualGroup = new Group();
        dualGroup.setAppName(appName);
        dualGroup.setUnitName(this.unitName);
        dualGroup.setName("DUAL_GROUP");
        dualGroup.setType(GroupType.DEMO);
        matrix.getGroups().add(dualGroup);

        if (subApps != null) {
            for (App subApp : subApps) {
                try {
                    TopologyHandler subTopologyHandler = new TopologyHandler(subApp.getAppName(),
                        this.unitName,
                        subApp.getTopologyFile(),
                        this.cp);
                    subTopologyHandler.init();
                    this.subTopologyHandlers.add(subTopologyHandler);

                    matrix.addSubMatrix(subTopologyHandler.getMatrix());
                } catch (TddlException e) {
                    logger.error("sub app topology init error, sub app is :" + subApp, e);
                }

            }
        }
        return matrix;
    }

    @Override
    protected void doDestroy() throws TddlException {
        Map<String, IRepository> repos = repositoryHolder.getRepository();
        for (IRepository repo : repos.values()) {
            repo.destroy();
        }

        if (topologyFileHandler != null) {
            topologyFileHandler.destroy();
        }

        for (ConfigDataHandler cdh : cdhs.asMap().values()) {
            cdh.destroy();
        }

        cdhs.cleanUp();
    }

    /**
     * 指定Group配置，创建一个GroupExecutor
     * 
     * @param group
     * @return
     */
    public IGroupExecutor createOne(Group group) {
        group.setAppName(this.appName);
        group.setUnitName(this.unitName);
        IRepository repo = repositoryHolder.getOrCreateRepository(group, matrix.getProperties(), cp);

        IGroupExecutor groupExecutor = repo.getGroupExecutor(group);
        putOne(group.getName(), groupExecutor);
        return groupExecutor;
    }

    /**
     * 添加指定groupKey的GroupExecutor，返回之前已有的
     * 
     * @param groupKey
     * @param groupExecutor
     * @return
     */
    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor) {
        return putOne(groupKey, groupExecutor, true);
    }

    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor, boolean singleton) {
        if (singleton && executorMap.containsKey(groupKey)) {
            throw new IllegalArgumentException("group key is already exists . group key : " + groupKey + " . map "
                                               + executorMap);
        }
        return executorMap.put(groupKey, groupExecutor);
    }

    public IGroupExecutor get(String key) {
        IGroupExecutor groupExecutor = executorMap.get(key);
        if (groupExecutor == null) {

            for (TopologyHandler sub : this.subTopologyHandlers) {
                groupExecutor = sub.get(key);

                if (groupExecutor != null) {
                    return groupExecutor;
                }
            }
            Group group = matrix.getGroup(key);
            if (group != null) {
                synchronized (executorMap) {
                    // double-check，避免并发创建
                    groupExecutor = executorMap.get(key);
                    if (groupExecutor == null) {
                        return createOne(group);
                    } else {
                        return executorMap.get(key);
                    }
                }
            }
        }

        return groupExecutor;
    }

    private String generateTopologyXML(final String appName, final String unitName) {
        try {
            final String matrixKey = GROUP_TOPOLOGY.format(new Object[] { appName });
            ConfigDataHandler cdh = cdhs.get(matrixKey, new Callable<ConfigDataHandler>() {

                @Override
                public ConfigDataHandler call() throws Exception {
                    return ConfigDataHandlerCity.getFactory(appName, unitName).getConfigDataHandler(matrixKey,
                        topologyListener);
                }
            });

            String keys = cdh.getData();
            if (keys == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG_MISS_TOPOLOGY, "dataId : " + matrixKey + " is null");
            }

            String[] keysArray = keys.split(",");
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document doc = builder.newDocument();

            Element matrix = doc.createElement("matrix");
            // matrix.setAttribute("name", appName);

            doc.appendChild(matrix); // 将根元素添加到文档上
            Element appNameNode = doc.createElement("appName");
            appNameNode.appendChild(doc.createTextNode(appName));
            matrix.appendChild(appNameNode);

            for (String str : keysArray) {
                Element group = doc.createElement("group");
                group.setAttribute("name", str);
                group.setAttribute("type", GroupType.MYSQL_JDBC.name());// 默认为mysql类型
                matrix.appendChild(group);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamWriter outwriter = new OutputStreamWriter(baos);
            XmlHelper.callWriteXmlFile(doc, outwriter, "utf-8");
            outwriter.close();
            String xml = baos.toString();
            return StringUtils.replace(xml, "<matrix>", xmlHead);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

    public class TopologyListener implements ConfigDataListener {

        private final TopologyHandler topologyHandler;

        public TopologyListener(TopologyHandler topologyHandler){
            this.topologyHandler = topologyHandler;
        }

        @Override
        public void onDataRecieved(String dataId, String data) {
            LoggerInit.TDDL_DYNAMIC_CONFIG.info("receive matrix dataId:" + dataId + " , data:" + data);
            synchronized (topologyHandler) {
                // 重载一次所有配置
                topologyHandler.matrix = topologyHandler.initMatrix(appName, topologyFilePath);
            }
        }
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getTopologyFilePath() {
        return topologyFilePath;
    }

    public void setTopologyFilePath(String topologyFilePath) {
        this.topologyFilePath = topologyFilePath;
    }

    public Matrix getMatrix() {
        return this.matrix;
    }

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

}
