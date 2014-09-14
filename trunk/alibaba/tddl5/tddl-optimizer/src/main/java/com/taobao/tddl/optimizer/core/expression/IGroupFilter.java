package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

/**
 * group filter, ie: (id = 1 or id = 3 or id is null) <br/>
 * 将多个相同列的or filter条件做为一个整体，进行下推处理
 * 
 * @author jianghang 2014-7-3 下午3:15:27
 * @since 5.1.7
 */
public interface IGroupFilter extends IFilter<IGroupFilter> {

    public Object getColumn();

    public IGroupFilter setColumn(Object column);

    public List<IFilter> getSubFilter();

    public IGroupFilter setSubFilter(List<IFilter> subFilters);

    public IGroupFilter addSubFilter(IFilter subFilter);
}
