package com.taobao.tddl.common.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 类似tddl三层结构的matrix概念，用于以后扩展第三方存储，目前扩展属性暂时使用properties代替
 * 
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class Matrix {

    private String              name;
    private List<Group>         groups     = new ArrayList<Group>();
    private Map<String, String> properties = new HashMap();
    private List<Matrix>        subMatrixs = new ArrayList();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    public Group getGroup(String groupName) {
        for (Group group : groups) {
            if (group.getName().equals(groupName)) {
                return group;
            }
        }

        for (Matrix subMatrix : this.subMatrixs) {
            Group group = subMatrix.getGroup(groupName);

            if (group != null) {
                return group;
            }
        }

        return null;
        // throw new TddlRuntimeException("not found groupName : " + groupName);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addSubMatrix(Matrix sub) {
        this.subMatrixs.add(sub);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
