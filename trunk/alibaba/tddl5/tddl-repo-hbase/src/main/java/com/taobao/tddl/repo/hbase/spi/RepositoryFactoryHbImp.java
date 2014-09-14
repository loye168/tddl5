package com.taobao.tddl.repo.hbase.spi;

import java.text.MessageFormat;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

@Activate(name = "HBASE_CLIENT")
public class RepositoryFactoryHbImp implements IRepositoryFactory {

    final static Log                  logger             = LogFactory.getLog(RepositoryFactoryHbImp.class);
    public final static MessageFormat HBASE_MAPPING_FILE = new MessageFormat("com.taobao.and_orV0.{0}_hbase_mapping");

    @Override
    public IRepository buildRepository(Group group, Map repoProperties, Map connectionProperties) {
        // 依赖一个mapping文件
        String mappingFile = GeneralUtil.getExtraCmdString(connectionProperties,
            ConnectionProperties.HBASE_MAPPING_FILE);

        String mappingData = null;
        ConfigDataHandler cdh = null;

        if (mappingFile != null) {
            cdh = ConfigDataHandlerCity.getFileFactory(group.getAppName()).getConfigDataHandler(mappingFile, null);
        } else {
            cdh = ConfigDataHandlerCity.getFactory(group.getAppName(), group.getUnitName())
                .getConfigDataHandler(HBASE_MAPPING_FILE.format(new Object[] { group.getAppName() }), null);
        }

        mappingData = cdh.getData();
        if (mappingData == null) {
            logger.error("init hbase mapping file failed! content is null");
            return null;
        }

        HbRepository hbRepo = new HbRepository(mappingData);
        try {
            hbRepo.init();
        } catch (TddlException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return hbRepo;
    }
}
