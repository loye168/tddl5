package com.taobao.tddl.common.utils.version;

import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

import com.taobao.tddl.common.client.util.ThreadLocalMap;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public final class Version {

    private Version(){
    }

    private static final Logger            logger    = LoggerFactory.getLogger(Version.class);
    private static final Package           myPackage = VersionAnnotation.class.getPackage();
    private static final VersionAnnotation va        = myPackage.getAnnotation(VersionAnnotation.class);
    private static final String            VERSION   = getVersion(Version.class, "5.1.7");

    static {
        // 检查是否存在重复的jar包
        // Version为tddl5.x之后的版本检测类
        // 如果Version检测没有发现重复，再和tddl3.x系列进行检查,ThreadLocalMap兼容了tddl3.x的类路径
        if (!Version.checkDuplicate(Version.class)) {
            Version.checkDuplicate(ThreadLocalMap.class);
        }

        // 检查下经常性冲突的两个包
        Version.checkDuplicate("com/alibaba/druid/pool/DruidDataSource.class", false);
        validVersion("druid", "com/alibaba/druid/pool/DruidDataSource.class", "1.0.6");
        Version.checkDuplicate("com/taobao/diamond/client/Diamond.class", false);
        validVersion("diamond", "com/taobao/diamond/client/Diamond.class", "3.6.8");
        Version.checkDuplicate("com/google/common/collect/MapMaker.class", false);
        validVersion("guava", "com/google/common/collect/MapMaker.class", "15.0");
    }

    public static String getVersion() {
        return VERSION;
    }

    /**
     * Returns the detail version info
     */
    public static String getBuildVersion() {
        StringBuilder buf = new StringBuilder();
        buf.append(SystemUtils.LINE_SEPARATOR);
        buf.append("[TDDL Version Info]").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[version ]").append(VERSION).append(SystemUtils.LINE_SEPARATOR);
        buf.append("[revision]").append(va != null ? va.revision() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[date    ]").append(va != null ? va.date() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[url     ]").append(va != null ? va.url() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[branch  ]").append(va != null ? va.branch() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[checksum]").append(va != null ? va.srcChecksum() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        return buf.toString();
    }

    public static String getVersion(Class<?> cls, String defaultVersion) {
        if (va != null) {
            defaultVersion = va.version();
        }

        try {
            // 首先查找MANIFEST.MF规范中的版本号
            String version = cls.getPackage().getImplementationVersion();
            if (version == null || version.length() == 0) {
                version = cls.getPackage().getSpecificationVersion();
            }
            if (version == null || version.length() == 0) {
                // 如果规范中没有版本号，基于jar包名获取版本号
                CodeSource codeSource = cls.getProtectionDomain().getCodeSource();
                if (codeSource == null) {
                    logger.info("No codeSource for class " + cls.getName() + " when getVersion, use default version "
                                + defaultVersion);
                } else {
                    String file = codeSource.getLocation().getFile();
                    version = getVerionByPath(file);
                }
            }

            if (checkVersionNecessary(version)) {
                // 返回版本号，如果为空返回缺省版本号
                return version == null || version.length() == 0 ? defaultVersion : version;
            } else {
                return defaultVersion;
            }
        } catch (Throwable e) { // 防御性容错
            // 忽略异常，返回缺省版本号
            logger.error("return default version, ignore exception " + e.getMessage(), e);
            return defaultVersion;
        }
    }

    /**
     * 检查下对应class path的版本，是否>minVersion
     */
    public static boolean validVersion(String name, String path, String minVersion) {
        try {
            if (minVersion == null) {
                return true;
            }

            Long minv = convertVersion(minVersion);
            Enumeration<URL> urls = Version.class.getClassLoader().getResources(path);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        String version = getVerionByPath(file);
                        if (checkVersionNecessary(version)) {
                            Long ver = convertVersion(version);
                            if (ver < minv) {
                                throw new IllegalStateException("check " + name + " version is " + version + " <= "
                                                                + minVersion + "(the minimum version), please upgrade "
                                                                + name + " jar version");
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) { // 防御性容错
            logger.error(e.getMessage(), e);
        }

        return true;
    }

    public static boolean checkDuplicate(Class<?> cls, boolean failOnError) {
        return checkDuplicate(cls.getName().replace('.', '/') + ".class", failOnError);
    }

    public static boolean checkDuplicate(Class<?> cls) {
        return checkDuplicate(cls, false);
    }

    public static boolean checkDuplicate(String path, boolean failOnError) {
        try {
            // 在ClassPath搜文件
            Enumeration<URL> urls = Version.class.getClassLoader().getResources(path);
            Set<String> files = new HashSet<String>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        files.add(file);
                    }
                }
            }
            // 如果有多个，就表示重复
            if (files.size() > 1) {
                String error = "Duplicate class " + path + " in " + files.size() + " jar " + files;
                if (failOnError) {
                    throw new IllegalStateException(error);
                } else {
                    logger.error(error);
                }

                return true;
            }
        } catch (Throwable e) { // 防御性容错
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 根据jar包的路径，找到对应的版本号
     */
    public static String getVerionByPath(String file) {
        if (file != null && file.length() > 0 && StringUtils.contains(file, ".jar")) {
            int index = StringUtils.indexOf(file, ".jar");
            file = file.substring(0, index);
            int i = file.lastIndexOf('/');
            if (i >= 0) {
                file = file.substring(i + 1);
            }
            i = file.indexOf("-");
            if (i >= 0) {
                file = file.substring(i + 1);
            }
            while (file.length() > 0 && !Character.isDigit(file.charAt(0))) {
                i = file.indexOf("-");
                if (i >= 0) {
                    file = file.substring(i + 1);
                } else {
                    break;
                }
            }
            return file;
        } else {
            return null;
        }
    }

    public static Long convertVersion(String version) {
        String parts[] = StringUtils.split(version, '.');
        Long result = 0l;
        int i = 1;
        int size = parts.length > 4 ? parts.length : 4;
        for (String part : parts) {
            if (StringUtils.isNumeric(part)) {
                result += Long.valueOf(part) * Double.valueOf(Math.pow(100, (size - i))).longValue();
            } else {
                String subParts[] = StringUtils.split(part, '-');
                if (StringUtils.isNumeric(subParts[0])) {
                    result += Long.valueOf(subParts[0]) * Double.valueOf(Math.pow(100, (size - i))).longValue();
                }
            }

            i++;
        }

        return result;
    }

    private static boolean checkVersionNecessary(String versionStr) {
        return !(versionStr == null || StringUtils.contains(versionStr, "with-dependencies") || StringUtils.contains(versionStr,
            "storm"));
    }
}
