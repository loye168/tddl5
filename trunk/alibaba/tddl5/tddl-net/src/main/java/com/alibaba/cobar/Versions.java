package com.alibaba.cobar;

import com.taobao.tddl.common.utils.version.Version;

/**
 * @author xianmao.hexm
 */
public interface Versions {

    /** 协议版本 */
    byte   PROTOCOL_VERSION = 10;

    /** 服务器版本 */
    String SERVER_VERSION   = Version.getVersion();

}
