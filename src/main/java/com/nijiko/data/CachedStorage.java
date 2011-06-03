package com.nijiko.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.nijiko.permissions.EntryType;

public abstract class CachedStorage implements Storage {
    
    private final Map<String, Set<String>> permissions = new HashMap<String, Set<String>>();
    private final Map<String, LinkedHashSet<GroupWorld>> parents = new HashMap<String, LinkedHashSet<GroupWorld>>();
    private final Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
    
    public CachedStorage(Storage wrapped) {
        if (wrapped instanceof CachedStorage)
            throw new RuntimeException("No Cacheception, please.");
    }
    
    @Override
    public Set<String> getPermissions(String name) {
        Set<String> perms = permissions.get(name);
        if (perms == null) {
            perms = getWrapped().getPermissions(name);
            permissions.put(name, perms);
        }
        return perms;
    }

    @Override
    public void addPermission(String name, String permission) {
        if (permissions.get(name) == null) {
            permissions.put(name, new HashSet<String>());
        }
        permissions.get(name).add(permission);
        getWrapped().addPermission(name, permission);
    }

    @Override
    public void removePermission(String name, String permission) {
        if (permissions.get(name) == null) {
            permissions.put(name, new HashSet<String>());
        }
        permissions.get(name).remove(permission);
        getWrapped().removePermission(name, permission);
    }

    @Override
    public LinkedHashSet<GroupWorld> getParents(String name) {
        LinkedHashSet<GroupWorld> entryParents = parents.get(name);
        if (entryParents == null) {
            entryParents = getWrapped().getParents(name);
            parents.put(name, entryParents);
        }
        return entryParents;
    }

    @Override
    public void addParent(String name, String groupWorld, String groupName) {
        GroupWorld gw = new GroupWorld(groupWorld, groupName);
        if (parents.get(name) == null) {
            parents.put(name, new LinkedHashSet<GroupWorld>());
        }
        parents.get(name).add(gw);
        getWrapped().addParent(name, groupWorld, groupName);        
    }

    @Override
    public void removeParent(String name, String groupWorld, String groupName) {
        GroupWorld gw = new GroupWorld(groupWorld, groupName);
        if (parents.get(name) == null) {
            parents.put(name, new LinkedHashSet<GroupWorld>());
        }
        parents.get(name).remove(gw);
        getWrapped().removeParent(name, groupWorld, groupName);                
    }

    @Override
    public Set<String> getEntries() {
        return getWrapped().getEntries();
    }

    @Override
    public EntryType getType() {
        return getWrapped().getType();
    }

    @Override
    public boolean create(String name) {
        return getWrapped().create(name);
    }
    
    @Override
    public boolean delete(String name) {
        permissions.remove(name);
        parents.remove(name);
        data.remove(name);
        return getWrapped().delete(name);
    }

    @Override
    public String getWorld() {
        return getWrapped().getWorld();
    }

    @Override
    public void forceSave() {
        getWrapped().forceSave();
    }

    @Override
    public void save() {
        getWrapped().save();
    }

    @Override
    public void reload() {
        getWrapped().reload();
        permissions.clear();
        parents.clear();
        data.clear();
    }

    @Override
    public boolean isAutoSave() {
        return getWrapped().isAutoSave();
    }

    @Override
    public void setAutoSave(boolean autoSave) {
        getWrapped().setAutoSave(autoSave);
    }

    @Override
    public String getString(String name, String path) {
        if (data.get(name) == null) {
            data.put(name, new HashMap<String, Object>());
        } else {
            Object o = data.get(name).get(path);
            if (data.get(name).containsKey(path) && o == null)
                return null;
            if (o != null)
                return o.toString();
        }
        String str = getWrapped().getString(name, path);
        data.get(name).put(path, str);
        return str;
    }

    @Override
    public Integer getInt(String name, String path) {
        if (data.get(name) == null) {
            data.put(name, new HashMap<String, Object>());
        } else {
            Object o = data.get(name).get(path);
            if (data.get(name).containsKey(path) && o == null)
                return null;
            if (o instanceof Integer)
                return (Integer) o;
        }
        Integer val = getWrapped().getInt(name, path);
        data.get(name).put(path, val);
        return val;
    }

    @Override
    public Double getDouble(String name, String path) {
        if (data.get(name) == null) {
            data.put(name, new HashMap<String, Object>());
        } else {
            Object o = data.get(name).get(path);
            if (data.get(name).containsKey(path) && o == null)
                return null;
            if (o instanceof Double)
                return (Double) o;
        }
        Double val = getWrapped().getDouble(name, path);
        data.get(name).put(path, val);
        return val;
    }

    @Override
    public Boolean getBool(String name, String path) {
        if (data.get(name) == null) {
            data.put(name, new HashMap<String, Object>());
        } else {
            Object o = data.get(name).get(path);
            if (data.get(name).containsKey(path) && o == null)
                return null;
            if (o instanceof Boolean)
                return (Boolean) o;
        }
        Boolean val = getWrapped().getBool(name, path);
        data.get(name).put(path, val);
        return val;
    }

    @Override
    public void setData(String name, String path, Object o) {
        if (!(o instanceof Integer) && !(o instanceof Boolean) && !(o instanceof Double) && !(o instanceof String)) {
            throw new IllegalArgumentException("Only ints, bools, doubles and Strings are allowed!");
        }
        if (data.get(name) == null) {
            data.put(name, new HashMap<String, Object>());
        }
        data.get(name).put(path, o);
        getWrapped().setData(name, path, o);
    }

    @Override
    public void removeData(String name, String path) {
        if (data.get(name) != null) {
            data.get(name).remove(path);
        }
        getWrapped().removeData(name, path);
    }

    protected abstract Storage getWrapped();
}
