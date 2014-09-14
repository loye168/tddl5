package com.taobao.tddl.executor.repo;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

public class RepositoryHolder {

    private Map<String, IRepository> repository = new ConcurrentHashMap<String, IRepository>();

    public boolean containsKey(Object repoName) {
        return repository.containsKey(repoName);
    }

    public boolean containsValue(Object repoName) {
        return repository.containsValue(repoName);
    }

    public IRepository get(Object repoName) {
        return repository.get(repoName);
    }

    public IRepository getOrCreateRepository(Group groupData, Map<String, String> properties, Map connectionProperties) {
        String group = groupData.getType().name();
        if (get(group) != null) {
            return get(group);
        }

        synchronized (this) {
            if (get(group) == null) {
                IRepositoryFactory factory = getRepoFactory(group);
                IRepository repo = factory.buildRepository(groupData, properties, connectionProperties);
                try {
                    repo.init();
                } catch (TddlException e) {
                    throw new TddlNestableRuntimeException(e);
                }
                this.put(group.toString(), repo);
            }
        }

        return this.get(group.toString());
    }

    public IRepository getOrCreateRepository(String group, Map<String, String> properties, Map connectionProperties) {
        if (get(group) != null) {
            return get(group);
        }

        synchronized (this) {
            if (get(group) == null) {
                IRepositoryFactory factory = getRepoFactory(group);
                IRepository repo = factory.buildRepository(null, properties, connectionProperties);
                try {
                    repo.init();
                } catch (TddlException e) {
                    throw new TddlNestableRuntimeException(e);
                }
                this.put(group.toString(), repo);
            }
        }

        return this.get(group.toString());
    }

    private IRepositoryFactory getRepoFactory(String repoName) {
        return ExtensionLoader.load(IRepositoryFactory.class, repoName);
    }

    public IRepository put(String repoName, IRepository repo) {
        return repository.put(repoName, repo);
    }

    public Set<Entry<String, IRepository>> entrySet() {
        return repository.entrySet();
    }

    public Map<String, IRepository> getRepository() {
        return repository;
    }

    public void setRepository(Map<String, IRepository> reponsitory) {
        this.repository = reponsitory;
    }

}
