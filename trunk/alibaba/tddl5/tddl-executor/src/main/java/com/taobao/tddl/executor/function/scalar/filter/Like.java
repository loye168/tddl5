package com.taobao.tddl.executor.function.scalar.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Like extends Filter {

    Pattern pattern;
    String  tarCache;

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        if (args[1] == null || args[0] == null) {
            return false;
        }
        String left = DataType.StringType.convertFrom(args[0]);
        String right = DataType.StringType.convertFrom(args[1]);

        if (tarCache == null || !tarCache.equals(right)) {
            if (pattern != null) {
                throw new IllegalArgumentException("should not be here");
            }

            tarCache = right;
            // trim and remove %%
            right = TStringUtil.trim(right);
            right = TStringUtil.replace(right, "\\_", "[uANDOR]");
            right = TStringUtil.replace(right, "\\%", "[pANDOR]");
            right = TStringUtil.replace(right, "%", ".*");
            right = TStringUtil.replace(right, "_", ".");
            right = TStringUtil.replace(right, "[uANDOR]", "\\_");
            right = TStringUtil.replace(right, "[pANDOR]", "\\%");

            // case insensitive
            right = "(?i)" + right;

            right = "^" + right;
            right = right + "$";

            pattern = Pattern.compile(right);

        }
        Matcher matcher = pattern.matcher(left);
        return matcher.find();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LIKE" };
    }

}
