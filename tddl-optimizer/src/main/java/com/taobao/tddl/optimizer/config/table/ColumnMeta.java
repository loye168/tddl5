package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Column 的元信息描述
 * 
 * @author whisper
 */
public class ColumnMeta implements Serializable {

    private static final long serialVersionUID = 1748510851861759314L;

    /**
     * 表名
     */
    private final String      tableName;

    /**
     * 列名
     */
    protected final String    name;

    /**
     * 当前列的类型
     */
    protected final DataType  dataType;

    /**
     * 当前列的别名
     */
    protected final String    alias;

    /**
     * 是否准许为空
     */
    protected final boolean   nullable;

    /**
     * 是否为自增
     */
    protected final boolean   autoIncrement;

    private String            fullName;

    public ColumnMeta(String tableName, String name, DataType dataType, String alias, boolean nullable){
        this.tableName = StringUtils.upperCase(tableName);
        this.name = StringUtils.upperCase(name);
        this.alias = StringUtils.upperCase(alias);
        this.dataType = dataType;
        this.nullable = nullable;
        this.autoIncrement = false;
    }

    public ColumnMeta(String tableName, String name, DataType dataType, String alias, boolean nullable,
                      boolean autoIncrement){
        this.tableName = StringUtils.upperCase(tableName);
        this.name = StringUtils.upperCase(name);
        this.alias = StringUtils.upperCase(alias);
        this.dataType = dataType;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alias == null) ? 0 : alias.hashCode());
        result = prime * result + (autoIncrement ? 1231 : 1237);
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnMeta other = (ColumnMeta) obj;
        if (alias == null) {
            if (other.alias != null) return false;
        } else if (!alias.equals(other.alias)) return false;
        if (autoIncrement != other.autoIncrement) return false;
        if (dataType == null) {
            if (other.dataType != null) return false;
        } else if (!dataType.equals(other.dataType)) return false;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (nullable != other.nullable) return false;
        if (tableName == null) {
            if (other.tableName != null) return false;
        } else if (!tableName.equals(other.tableName)) return false;
        return true;
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append(tableName).append(".");
        sb.append(name);
        if (alias != null) {
            sb.append(" as ").append(alias);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String getFullName() {
        if (this.fullName == null) {
            String cn = this.getAlias() != null ? this.getAlias() : this.getName();
            String tableName = this.getTableName() == null ? "" : this.getTableName();
            StringBuilder sb = new StringBuilder(tableName.length() + 1 + cn.length());
            sb.append(this.getTableName());
            sb.append('.');
            sb.append(cn);

            this.fullName = sb.toString();
        }
        return this.fullName;
    }

}
