package com.nijikokun.bukkit.Permissions;

import org.bukkit.event.CustomEventListener;
import org.bukkit.event.Event;

import com.nijiko.data.SqlStorage;

public class PermissionsListener extends CustomEventListener {
    @Override
    public void onCustomEvent(Event event) {
        String name = event.getEventName();
        if("StorageReloadEvent".equals(name)) {
            SqlStorage.clearWorldCache();
        } else if("ControlCloseEvent".equals(name)) {
            SqlStorage.closeAll();
        }
    }
}
