package com.nijiko.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nijiko.data.SqlStorage.NameWorldId;
import com.nijiko.permissions.EntryType;

public abstract class SqlEntryStorage implements Storage {

    protected static final int max = 5;
    protected final String world;
    protected int worldId;
    protected Map<String, Integer> idCache = new HashMap<String, Integer>();

    protected static final String permGetText = "SELECT permstring FROM PrPermissions WHERE entryid = ?;";
    static PreparedStatementPool permGetPool;
    protected static final String parentGetText = "SELECT parentid FROM PrInheritance WHERE childid = ? ORDER BY parentorder;";
    static PreparedStatementPool parentGetPool;

    protected static final String permAddText = "INSERT IGNORE INTO PrPermissions (entryid, permstring) VALUES (?,?);";
    static PreparedStatementPool permAddPool;
    protected static final String permRemText = "DELETE FROM PrPermissions WHERE entryid = ? AND permstring = ?;";
    static PreparedStatementPool permRemPool;
    protected static final String parentAddText = "INSERT IGNORE INTO PrInheritance (childid, parentid) VALUES (?,?);";
    static PreparedStatementPool parentAddPool;
    protected static final String parentRemText = "DELETE FROM PrInheritance WHERE childid = ? AND parentid = ?;";
    static PreparedStatementPool parentRemPool;

    protected static final String entryListText = "SELECT name, entryid FROM PrEntries WHERE worldid = ? AND type = ?;";
    static PreparedStatementPool entryListPool;
    protected static final String entryDelText = "DELETE FROM PrEntries WHERE worldid = ? AND entryid = ?;";
    static PreparedStatementPool entryDelPool;

    protected static final String dataGetText = "SELECT * FROM PrData WHERE entryid = ? AND path = ?;";
    static PreparedStatementPool dataGetPool;
    protected static final String dataModText = "REPLACE INTO PrData (data, entryid, path) VALUES (?,?,?);";
    static PreparedStatementPool dataModPool;
    protected static final String dataDelText = "DELETE FROM PrData WHERE entryid = ? AND path = ?;";
    static PreparedStatementPool dataDelPool;

    static void reloadPools(Connection dbConn) {
        Dbms dbms = SqlStorage.getDbms();
        permGetPool = new PreparedStatementPool(dbConn, permGetText, max);
        parentGetPool = new PreparedStatementPool(dbConn, parentGetText, max);
        permAddPool = new PreparedStatementPool(dbConn, (dbms == Dbms.SQLITE ? permAddText.replace("IGNORE", "OR IGNORE") : permAddText), max);
        permRemPool = new PreparedStatementPool(dbConn, permRemText, max);
        parentAddPool = new PreparedStatementPool(dbConn, (dbms == Dbms.SQLITE ? parentAddText.replace("IGNORE", "OR IGNORE") : parentAddText), max);
        parentRemPool = new PreparedStatementPool(dbConn, parentRemText, max);
        entryListPool = new PreparedStatementPool(dbConn, entryListText, max);
        entryDelPool = new PreparedStatementPool(dbConn, entryDelText, max);
        dataModPool = new PreparedStatementPool(dbConn, dataModText, max);
        dataDelPool = new PreparedStatementPool(dbConn, dataDelText, max);
        dataGetPool = new PreparedStatementPool(dbConn, dataGetText, max);
    }

    public SqlEntryStorage(String world, int id) {
        worldId = id;
        this.world = world;
        reload();
    }

    @Override
    public Set<String> getPermissions(String name) {
        Set<String> permissions = new HashSet<String>();
        if (name != null) {
            int id;
            try {
                id = getId(name);
            } catch (SQLException e) {
                e.printStackTrace();
                return permissions;
            }
            List<Map<Integer, Object>> results = SqlStorage.runQuery(permGetPool, new Object[] { id }, false, 1);
            if (results != null) {
                for (Map<Integer, Object> row : results) {
                    Object o = row.get(1);
                    if (o instanceof String) {
                        permissions.add((String) o);
                    }
                }
            }
        }
        return permissions;
    }

    @Override
    public LinkedHashSet<GroupWorld> getParents(String name) {
        LinkedHashSet<GroupWorld> parents = new LinkedHashSet<GroupWorld>();
        if (name != null) {
            int uid;
            try {
                uid = getId(name);
            } catch (SQLException e) {
                e.printStackTrace();
                return parents;
            }
            List<Map<Integer, Object>> results = SqlStorage.runQuery(parentGetPool, new Object[] { uid }, false, 1);
            if (results != null) {
                for (Map<Integer, Object> row : results) {
                    Object o = row.get(1);
                    if (o instanceof Integer) {
                        int groupid = (Integer) o;
                        NameWorldId nw;
                        String worldName;
                        nw = SqlStorage.getEntryName(groupid);
                        worldName = SqlStorage.getWorldName(nw.worldid);
                        GroupWorld gw = new GroupWorld(worldName, nw.name);
                        parents.add(gw);
                    }
                }
            }
        }
        return parents;
    }

    @Override
    public void addPermission(String name, String permission) {
        int uid;
        try {
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(permAddPool, new Object[] { uid, permission });
    }

    @Override
    public void removePermission(String name, String permission) {
        int uid;
        try {
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(permRemPool, new Object[] { uid, permission });
    }

    @Override
    public void addParent(String name, String groupWorld, String groupName) {
        int uid;
        int gid;
        try {
            gid = SqlStorage.getEntry(groupWorld, groupName, true);
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(parentAddPool, new Object[] { uid, gid });
    }

    @Override
    public void removeParent(String name, String groupWorld, String groupName) {
        int uid;
        int gid;
        try {
            gid = SqlStorage.getEntry(groupWorld, groupName, true);
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(parentRemPool, new Object[] { uid, gid });
    }

    @Override
    public Set<String> getEntries() {
        if (idCache.isEmpty()) {
            List<Map<Integer, Object>> results = SqlStorage.runQuery(entryListPool, new Object[] { worldId, (byte) (this.getType() == EntryType.GROUP ? 1 : 0) }, false, 1, 2);
            for (Map<Integer, Object> row : results) {
                Object oName = row.get(1);
                Object oId = row.get(2);
                if (oName instanceof String && oId instanceof Integer) {
                    idCache.put((String) oName, (Integer) oId);
                }
            }
        }
        return idCache.keySet();
    }

    @Override
    public String getWorld() {
        return world;
    }

    @Override
    public void forceSave() {
        return;
    }

    @Override
    public void save() {
        return;
    }

    @Override
    public void reload() {
        idCache.clear();
    }

    @Override
    public boolean isAutoSave() {
        return true;
    }

    @Override
    public void setAutoSave(boolean autoSave) {
        return;
    }

    @Override
    public boolean create(String name) {
        if (!idCache.containsKey(name)) {
            int id = SqlStorage.getEntry(world, name, this.getType() == EntryType.GROUP);
            idCache.put(name, id);
            return true;
        }
        return false;
    }
    
    @Override
    public boolean delete(String name) {
        int id = idCache.remove(name);
        int val = SqlStorage.runUpdate(entryDelPool, new Object[] {worldId, id});
        return val != 0;        
    }

    @Override
    public String getString(String name, String path) {
        String data = null;
        int uid;
        try {
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return data;
        }
        List<Map<Integer, Object>> results = SqlStorage.runQuery(dataGetPool, new Object[] { uid, path }, true, 1, 2);
        for (Map<Integer, Object> row : results) {
            Object o = row.get(1);
            if (o instanceof String) {
                data = (String) o;
            }
        }
        return data;
    }

    @Override
    public Integer getInt(String name, String path) {
        String raw = getString(name, path);
        Integer value;
        try {
            value = Integer.valueOf(raw);
        } catch (NumberFormatException e) {
            value = null;
        }
        return value;
    }

    @Override
    public Double getDouble(String name, String path) {
        String raw = getString(name, path);
        Double value;
        try {
            value = Double.valueOf(raw);
        } catch (NumberFormatException e) {
            value = null;
        }
        return value;
    }

    @Override
    public Boolean getBool(String name, String path) {
        String raw = getString(name, path);
        Boolean value;
        try {
            value = Boolean.valueOf(raw);
        } catch (NumberFormatException e) {
            value = null;
        }
        return value;
    }

    @Override
    public void setData(String name, String path, Object data) {
        String szForm = "";
        if (data instanceof Integer) {
            szForm = ((Integer) data).toString();
        } else if (data instanceof Boolean) {
            szForm = ((Boolean) data).toString();
        } else if (data instanceof Double) {
            szForm = ((Double) data).toString();
        } else if (data instanceof String) {
            szForm = (String) data;
        } else {
            throw new IllegalArgumentException("Only ints, bools, doubles and Strings are allowed!");
        }
        int uid;
        try {
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(dataModPool, new Object[] { szForm, uid, path });
    }

    @Override
    public void removeData(String name, String path) {
        int uid;
        try {
            uid = getId(name);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
        SqlStorage.runUpdate(dataDelPool, new Object[] { uid, path });
    }

    public static void close() {
        parentGetPool.close();
        parentAddPool.close();
        parentRemPool.close();
        permGetPool.close();
        permAddPool.close();
        permRemPool.close();
        dataGetPool.close();
        dataModPool.close();
        dataDelPool.close();
        entryListPool.close();
    }

    public Integer getCachedId(String name) {
        return idCache.get(name);
    }

    protected abstract int getId(String name) throws SQLException;

}
