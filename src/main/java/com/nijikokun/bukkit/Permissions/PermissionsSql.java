package com.nijikokun.bukkit.Permissions;

import java.util.logging.Logger;

import org.bukkit.event.Event.Priority;
import org.bukkit.event.Event.Type;
import org.bukkit.plugin.java.JavaPlugin;

import com.nijiko.data.SqlCreator;
import com.nijiko.data.StorageFactory;

public class PermissionsSql extends JavaPlugin {

    private final PermissionsListener pListener = new PermissionsListener();
    public final Logger log = Logger.getLogger("Minecraft");
    private final SqlCreator creator = new SqlCreator();

    public PermissionsSql () {
        StorageFactory.registerCreator("SQL", creator);
    }
    @Override
    public void onDisable() {
        StorageFactory.unregisterCreator("SQL", creator);
        log.info("[Permissions] SQL Interface disabled!");
    }

    @Override
    public void onEnable() {
        StorageFactory.registerCreator("SQL", creator);
        getServer().getPluginManager().registerEvent(Type.CUSTOM_EVENT, pListener, Priority.Monitor, this);
        log.info("[Permissions] SQL Interface enabled!");

    }

}
