/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2010 Oracle.  All rights reserved.
 *
 */

package com.taobao.tddl.common.properties;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class ParamManager {

    /*
     * The name of the JE properties file, to be found in the environment
     * directory.
     */
    public static final String    PROPFILE_NAME = "tddl.properties";

    /*
     * All properties in effect for this JE instance, both environment and
     * replication environment scoped, are stored in this Map field.
     */
    protected Map<String, String> props         = new HashMap<String, String>();

    public ParamManager(Map connectionMap){
        if (connectionMap != null) {
            props = connectionMap;
        }
    }

    /*
     * Parameter Access
     */

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if param wasn't explicitly set
     */
    public synchronized String get(ConfigParam configParam) {
        return getConfigParam(props, configParam.getName());
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if param wasn't explicitly set
     */
    public synchronized String get(String configParamName) {
        return getConfigParam(props, configParamName);
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if it wasn't explicitly set.
     */
    public boolean getBoolean(BooleanConfigParam configParam) {

        /* See if it's specified. */
        String val = get(configParam);
        return Boolean.valueOf(val).booleanValue();
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if it wasn't explicitly set.
     */
    public short getShort(ShortConfigParam configParam) {

        /* See if it's specified. */
        String val = get(configParam);
        short shortValue = 0;
        if (val != null) {
            try {
                shortValue = Short.parseShort(val);
            } catch (NumberFormatException e) {

                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */
                assert false : e.getMessage();
            }
        }
        return shortValue;
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if it wasn't explicitly set.
     */
    public int getInt(IntConfigParam configParam) {

        /* See if it's specified. */
        String val = get(configParam);
        int intValue = 0;
        if (val != null) {
            try {
                intValue = Integer.parseInt(val);
            } catch (NumberFormatException e) {

                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */
                assert false : e.getMessage();
            }
        }
        return intValue;
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if it wasn't explicitly set
     */
    public long getLong(LongConfigParam configParam) {

        /* See if it's specified. */
        String val = get(configParam);
        long longValue = 0;
        if (val != null) {
            try {
                longValue = Long.parseLong(val);
            } catch (NumberFormatException e) {
                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */
                assert false : e.getMessage();
            }
        }
        return longValue;
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     * 
     * @param configParam
     * @return default for param if it wasn't explicitly set.
     */
    public int getDuration(DurationConfigParam configParam) {
        String val = get(configParam);
        int millis = 0;
        if (val != null) {
            try {
                millis = PropUtil.parseDuration(val);
            } catch (IllegalArgumentException e) {

                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */
                assert false : e.getMessage();
            }
        }
        return millis;
    }

    /*
     * Helper methods used by EnvironmentConfig and ReplicationConfig.
     */

    /**
     * Validate a collection of configurations, checking that - the name and
     * value are valid - a replication param is not being set through an
     * EnvironmentConfig class, and a non-rep param is not set through a
     * ReplicationConfig instance. This may happen at Environment start time, or
     * when configurations have been mutated. The configurations have been
     * collected from a file, or from a Map object, and haven't gone through the
     * usual validation path that occurs when XXXConfig.setConfigParam is
     * called. SuppressWarnings is used here because Enumeration doesn't work
     * well with Map in Java 1.5
     * 
     * @throws IllegalArgumentException via XxxConfig(Map) ctor.
     */
    @SuppressWarnings("unchecked")
    public static void validateMap(Map<String, String> props, boolean isRepConfigInstance, String configClassName)
                                                                                                                  throws IllegalArgumentException {

        /* Check that the properties have valid names and values. */
        Iterator propNames = props.keySet().iterator();
        while (propNames.hasNext()) {
            String name = (String) propNames.next();
            /* Is this a valid property name? */
            ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(name);

            if (param == null) {
                /* See if the parameter is an multi-value parameter. */
                String mvParamName = ConfigParam.multiValueParamName(name);
                param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);

                if (param == null) {

                    /*
                     * Remove the property only if: 1. The parameter name
                     * indicates it's a replication parameter 2. The Environment
                     * is being opened in standalone mode 3. The parameter is
                     * being initialized in the properties file See SR [#19080].
                     */
                    if (configClassName == null && !isRepConfigInstance) {
                        props.remove(name);
                        continue;
                    }

                    throw new IllegalArgumentException(name + " is not a valid BDBJE environment configuration");
                }
            }

            /* Is this a valid property value? */
            param.validateValue(props.get(name));
        }
    }

    /**
     * Helper method for environment and replication configuration classes. Set
     * a configuration parameter. Check that the name is valid. If specified,
     * also check that the value is valid.Value checking may be disabled for
     * unit testing.
     * 
     * @param props Property bag held within the configuration object.
     * @throws IllegalArgumentException via XxxConfig.setXxx methods and
     * XxxConfig(Map) ctor.
     */
    public static void setConfigParam(Map<String, String> props, String paramName, String value,
                                      boolean requireMutability, boolean validateValue, boolean forReplication,
                                      boolean verifyForReplication) throws IllegalArgumentException {

        boolean isMVParam = false;

        /* Is this a valid property name? */
        ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(paramName);

        if (param == null) {
            /* See if the parameter is an multi-value parameter. */
            String mvParamName = ConfigParam.multiValueParamName(paramName);
            param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);
            if (param == null || !param.isMultiValueParam()) {
                throw new IllegalArgumentException(paramName + " is not a valid BDBJE environment parameter");
            }
            isMVParam = true;
            assert param.isMultiValueParam();
        }

        /* Is this a mutable property? */
        if (requireMutability && !param.isMutable()) {
            throw new IllegalArgumentException(paramName + " is not a mutable BDBJE environment configuration");
        }

        if (isMVParam) {
            setVal(props, param, paramName, value, validateValue);
        } else {
            setVal(props, param, value, validateValue);
        }
    }

    /**
     * Helper method for environment and replication configuration classes. Get
     * the configuration value for the specified parameter, checking that the
     * parameter name is valid.
     * 
     * @param props Property bag held within the configuration object.
     * @throws IllegalArgumentException via XxxConfig.getConfigParam.
     */
    public static String getConfigParam(Map<String, String> props, String paramName) throws IllegalArgumentException {

        boolean isMVParam = false;

        /* Is this a valid property name? */
        ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(paramName);

        if (param == null) {

            /* See if the parameter is an multi-value parameter. */
            String mvParamName = ConfigParam.multiValueParamName(paramName);
            param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);
            if (param == null) {
                throw new IllegalArgumentException(paramName + " is not a valid BDBJE environment configuration");
            }
            isMVParam = true;
            assert param.isMultiValueParam();
        } else if (param.isMultiValueParam()) {
            throw new IllegalArgumentException("Use getMultiValueValues() to retrieve Multi-Value "
                                               + "parameter values.");
        }

        if (isMVParam) {
            return ParamManager.getVal(props, param, paramName);
        }
        return ParamManager.getVal(props, param);
    }

    /**
     * Helper method for environment and replication configuration classes. Gets
     * either the value stored in this configuration or the default value for
     * this param.
     */
    public static String getVal(Map<String, String> props, ConfigParam param) {
        String val = props.get(param.getName());
        if (val == null) {
            val = param.getDefault();
        }
        return val;
    }

    /**
     * Helper method for environment and replication configuration classes. Gets
     * either the value stored in this configuration or the default value for
     * this param.
     */
    public static String getVal(Map<String, String> props, ConfigParam param, String paramName) {
        String val = props.get(paramName);
        if (val == null) {
            val = param.getDefault();
        }
        return val;
    }

    /**
     * Helper method for environment and replication configuration classes. Set
     * and validate the value for the specified parameter.
     */
    public static void setVal(Map<String, String> props, ConfigParam param, String val, boolean validateValue)
                                                                                                              throws IllegalArgumentException {

        if (validateValue) {
            param.validateValue(val);
        }
        props.put(param.getName(), val);
    }

    /**
     * Helper method for environment and replication configuration classes. Set
     * and validate the value for the specified parameter.
     */
    public static void setVal(Map<String, String> props, ConfigParam param, String paramName, String val,
                              boolean validateValue) throws IllegalArgumentException {

        if (validateValue) {
            param.validateValue(val);
        }
        props.put(paramName, val);
    }

    /**
     * Helper method for getting integer values.
     */
    public static int getIntVal(Map<String, String> props, IntConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                                                                 + e.getMessage());
        }
    }

    /**
     * Helper method for getting long values.
     */
    public static long getLongVal(Map<String, String> props, LongConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                                                                 + e.getMessage());
        }
    }

    /**
     * Helper method for setting integer values.
     */
    public static void setIntVal(Map<String, String> props, IntConfigParam param, int val, boolean validateValue) {
        setVal(props, param, Integer.toString(val), validateValue);
    }

    /**
     * Helper method for getting boolean values.
     */
    public static boolean getBooleanVal(Map<String, String> props, BooleanConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        return Boolean.parseBoolean(val);
    }

    /**
     * Helper method for setting boolean values.
     */
    public static void setBooleanVal(Map<String, String> props, BooleanConfigParam param, boolean val,
                                     boolean validateValue) {
        setVal(props, param, Boolean.toString(val), validateValue);
    }

    /**
     * Helper method for getting duration values.
     */
    public static long getDurationVal(Map<String, String> props, DurationConfigParam param, TimeUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("TimeUnit argument may not be null");
        }
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return unit.convert(PropUtil.parseDuration(val), TimeUnit.MILLISECONDS);
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                                                                 + e.getMessage());
        }
    }

    /**
     * Helper method for setting duration values.
     */
    public static void setDurationVal(Map<String, String> props, DurationConfigParam param, long val, TimeUnit unit,
                                      boolean validateValue) {
        setVal(props, param, PropUtil.formatDuration(val, unit), validateValue);
    }
}
