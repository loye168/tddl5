package com.alibaba.cobar.manager.dataobject.xml;

/**
 * @author haiqing.zhuhq 2011-6-17
 */
public class UserDO {

    private long   id;
    private String realname;
    private String username;
    private String password;
    private String user_role;
    private String status;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getRealname() {
        return realname;
    }

    public void setRealname(String realname) {
        this.realname = realname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser_role() {
        return user_role;
    }

    public void setUser_role(String user_role) {
        this.user_role = user_role;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("id:")
            .append(id)
            .append("|realname:")
            .append(realname)
            .append("|username")
            .append(username)
            .append("|password:")
            .append(password)
            .append("|user_role:")
            .append(user_role)
            .append("|status:")
            .append(status)
            .toString();
    }

}
