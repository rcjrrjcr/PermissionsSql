package com.nijiko.data;

import java.io.File;

import org.bukkit.util.config.Configuration;

import com.nijikokun.bukkit.Permissions.Permissions;

public class SqlCreator implements StorageCreator {

    @Override
    public UserStorage getUserStorage(String world, int reload, boolean autosave, Configuration config) throws Exception {
        String dbms = config.getString("permissions.storage.dbms", "SQLITE");
        String uri = config.getString("permissions.storage.uri", "jdbc:sqlite:" + Permissions.instance.getDataFolder() + File.separator + "permissions.db");
        String username = config.getString("permissions.storage.username");
        String password = config.getString("permissions.storage.password");
        boolean cached = config.getBoolean("permissions.storage.cache", true);

        SqlStorage.init(dbms, uri, username, password, reload);
        SqlUserStorage sus = SqlStorage.getUserStorage(world);
        if (cached)
            return new CachedUserStorage(sus);
        else
            return sus;
    }

    @Override
    public GroupStorage getGroupStorage(String world, int reload, boolean autosave, Configuration config) throws Exception {
        String dbms = config.getString("permissions.storage.dbms", "SQLITE");
        String uri = config.getString("permissions.storage.uri", "jdbc:sqlite:" + Permissions.instance.getDataFolder() + File.separator + "permissions.db");
        String username = config.getString("permissions.storage.username");
        String password = config.getString("permissions.storage.password");
        boolean cached = config.getBoolean("permissions.storage.cache", true);
        
        SqlStorage.init(dbms, uri, username, password, reload);
        SqlGroupStorage sgs = SqlStorage.getGroupStorage(world);
        if (cached)
            return new CachedGroupStorage(sgs);
        else
            return sgs;
    }

}
