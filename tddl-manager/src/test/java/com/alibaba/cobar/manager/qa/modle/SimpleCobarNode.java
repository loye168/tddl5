package com.alibaba.cobar.manager.qa.modle;

import com.mysql.jdbc.Connection;

public class SimpleCobarNode extends SimpleMySqlNode {

    private int    dmlPort     = 0;
    private int    managerPort = 0;
    private String user;
    private String password;

    public SimpleCobarNode(String cobarIP, int dmlPort, int managerPort, String user, String password) throws Exception{
        super(cobarIP);
        this.dmlPort = dmlPort;
        this.managerPort = managerPort;
        this.user = user;
        this.password = password;
    }

    public Connection createDMLConnection(String schema) throws Exception {
        return createConnection(dmlPort, user, password, schema);
    }

    public Connection createManagerConnection() throws Exception {
        return createConnection(managerPort, user, password, "");
    }

}
