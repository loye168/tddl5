package com.alibaba.cobar.manager.dao;

import java.util.List;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;

public interface UserDAO {

    public UserDO validateUser(String username, String password);

    public boolean checkName(String username);

    public boolean checkName(String username, long userId);

    public List<UserDO> getUserList();

    public boolean addUser(UserDO user);

    public boolean modifyUser(UserDO user);

    public UserDO getUserById(long id);

}
