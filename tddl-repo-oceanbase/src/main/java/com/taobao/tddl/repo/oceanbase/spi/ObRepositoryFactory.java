package com.taobao.tddl.repo.oceanbase.spi;

import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

@Activate(name = "OCEANBASE_JDBC")
public class ObRepositoryFactory implements IRepositoryFactory {

    @Override
    public IRepository buildRepository(Group group, Map repoProperties, Map connectionProperties) {
        Ob_Repository myRepo = new Ob_Repository();
        try {
            myRepo.init();
        } catch (TddlException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return myRepo;
    }

}
