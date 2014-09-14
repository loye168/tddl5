/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2011 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.taobao.tddl.common.properties;

/**
 * A configuration parameter with an string value.
 */
public class StringConfigParam extends ConfigParam {

    public static final String DEBUG_NAME = StringConfigParam.class.getName();

    public StringConfigParam(String configName, String defaultValue, boolean mutable){
        /* defaultValue must not be null. */
        super(configName, defaultValue, mutable);

    }

    @Override
    public void validateValue(String value) throws IllegalArgumentException {
        // do nothing
    }
}
