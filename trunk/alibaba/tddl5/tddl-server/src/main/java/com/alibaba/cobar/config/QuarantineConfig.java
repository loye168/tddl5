package com.alibaba.cobar.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.server.util.StringUtil;

/**
 * 隔离区配置定义
 * 
 * @author haiqing.zhuhq 2012-4-17
 */
public final class QuarantineConfig {

    /**
     * Format is 10.20.0.1-10.20.255.255
     */
    static class NetScope {

        public long startAddr;
        public long endAddr;

        public NetScope(String inputScope) throws IllegalArgumentException{
            if (StringUtil.isEmpty(inputScope)) {
                throw new IllegalArgumentException("Input scope is empty!");
            }

            inputScope = inputScope.trim();

            int pos;
            if (-1 != (pos = inputScope.indexOf("-"))) {
                String start = inputScope.substring(0, pos);
                String end = inputScope.substring(pos + 1);
                end = end.replace("-", "");
                startAddr = ip2long(start);
                endAddr = ip2long(end);
            } else {
                throw new IllegalArgumentException("input: " + inputScope + " is not supported!");
            }
        }

        public boolean isInScope(long ip) {
            if ((ip <= endAddr) == (ip >= startAddr)) {
                return true;
            }
            return false;
        }
    }

    /**
     * Netmask may have several presentation 10.20.*.* 10.20.0.0/24 Should first
     * convert input ip to long then check if it's in the scope, since we use
     * simple string to store the Netmaks, it may be duplicated for one mask
     * defination. Do not need to store original mask string, since it will
     * always clear all content when set a new value.
     */
    static class Netmask {

        public long startAddr;
        public int  submask;

        public Netmask(String inputmask) throws IllegalArgumentException{
            if (StringUtil.isEmpty(inputmask)) {
                throw new IllegalArgumentException("Input maks is empty!");
            }

            inputmask = inputmask.trim();

            if (-1 != inputmask.indexOf("/")) {
                // format is 10.20.0.0/24
                int type = Integer.parseInt(inputmask.replaceAll(".*/", ""));
                int mask = 0xFFFFFFFF << (32 - type);
                String cidrIp = inputmask.replaceAll("/.*", "");
                startAddr = ip2long(cidrIp);
                submask = mask;
            } else if (-1 != inputmask.indexOf("*")) {
                // format is 10.20.*.*
                submask = 0xFFFFFFFF;
                String[] fourPlay = inputmask.split("\\.");
                for (int i = 0; i < fourPlay.length; i++) {
                    if (!fourPlay[i].equals("*")) {
                        startAddr += addressItem(inputmask, fourPlay[i]) << ((3 - i) * 8);
                    } else {
                        submask &= ~(0xFF << (3 - i) * 8);
                    }
                }
            } else {
                throw new IllegalArgumentException("input: " + inputmask + " is not supported!");
            }

        }

        public boolean isInScope(long ip) {
            if ((ip & submask) == (startAddr & submask)) {
                return true;
            }
            return false;
        }
    }

    class UserIpDefines implements IUserHostDefination {

        Set<String>    hosts;
        List<Netmask>  netmasks;
        List<NetScope> netscopes;

        public UserIpDefines(Set<String> hosts, List<Netmask> netmasks, List<NetScope> netscopes){
            this.hosts = hosts;
            this.netmasks = netmasks;
            this.netscopes = netscopes;
        }

        public boolean isEmpty() {
            return hosts.isEmpty() && netmasks.isEmpty() && netscopes.isEmpty();
        }

        /**
         * ordered by host --> mask --> scope
         * 
         * @param host
         * @return
         */
        public boolean containsOf(String host) {

            if (hosts.contains(host)) {
                return true;
            }

            long ip = ip2long(host);

            return containsOf(ip);
        }

        public boolean containsOf(long host) {
            for (int i = 0; i < netmasks.size(); i++) {
                if (netmasks.get(i).isInScope(host)) {
                    return true;
                }
            }

            for (int i = 0; i < netscopes.size(); i++) {
                if (netscopes.get(i).isInScope(host)) {
                    return true;
                }
            }

            return false;
        }
    }

    private final Map<String /* user/appName */, IUserHostDefination /* userIpDefines */> hosts;
    private IUserHostDefination clusterBl = null; /* black list for cluster, maybe null */
    private IUserHostDefination trustedips = null; /* trusted ip for internal usage */

    private static QuarantineConfig instance;
    static {
        instance = new QuarantineConfig();
    }

    static public QuarantineConfig getInstance() {
        return instance;
    }

    private QuarantineConfig(){
        hosts = new HashMap<String, IUserHostDefination>();
    }

    public Map<String, IUserHostDefination> getHosts() {
        return hosts;
    }

    public void cleanHostByApp(String app) {
        if (hosts.containsKey(app)) {
            hosts.remove(app);
        }
    }

    /**
     * No need to determine if .xxx. should in [0,255] because the request in ip
     * is generated by system, even if the definition is not so accurate, the
     * purpose should always be valid.
     * 
     * @param ip
     * @return
     */
    static long ip2long(String ip) {
        String[] ips = ip.split("\\.");
        long result = 0;
        for (int i = 0; i < ips.length; i++) {
            result += addressItem(ip, ips[i]) << ((3 - i) * 8);
        }

        return result;
    }

    static int addressItem(String orig, String item) throws IllegalArgumentException {
        int result = Integer.parseInt(item);

        if (result < 0 || result > 255) {
            throw new IllegalArgumentException("Address: " + orig + " is not valid");
        }

        return result;
    }

    /**
     * If we met format or addressItem invalid exception, then it will be thrown
     * and keep the original unchanged.
     * 
     * @param config
     * @return
     */
    private IUserHostDefination parseHostDefinations(String config) {
        HashSet<String> hostset = new HashSet<String>();
        ArrayList<NetScope> netscopes = new ArrayList<NetScope>();
        ArrayList<Netmask> netmasks = new ArrayList<Netmask>();
        if (config == null) {
            return new UserIpDefines(hostset, netmasks, netscopes);
        }

        String[] quarantinesByHost = StringUtil.split(config, ',', true);
        for (String host : quarantinesByHost) {
            if (-1 != host.indexOf("/") || -1 != host.indexOf("*")) {
                netmasks.add(new Netmask(host));
            } else if (-1 != host.indexOf("-")) {
                netscopes.add(new NetScope(host));
            } else {
                hostset.add(host);
                /**
                 * Put to quick hash set, no need to keep the sequence, if hosts
                 * is empty, then hostset will be empty as well;
                 */
            }
        }

        return new UserIpDefines(hostset, netmasks, netscopes);
    }

    public void resetTrustedIps(String config) {
        if (StringUtil.isEmpty(config)) {
            return;
        }
        this.trustedips = parseHostDefinations(config);
    }

    public void resetBlackList(String config) {
        if (StringUtil.isEmpty(config)) {
            return;
        }
        this.clusterBl = parseHostDefinations(config);
    }

    public void resetHosts(String app, String config) {
        this.hosts.put(app, parseHostDefinations(config));
    }

    public IUserHostDefination getClusterBlacklist() {
        return clusterBl;
    }

    public IUserHostDefination getTrustedIps() {
        return trustedips;
    }

    static public void main(String[] args)
    {
        Netmask test1 = new Netmask("10.20.*.*");
        Netmask test2 = new Netmask("10.20.0.0/24");
        NetScope test3 = new NetScope("10.20.0.1-10.20.0.10");
        long addr = ip2long("127.256.0.256");
    }

}
