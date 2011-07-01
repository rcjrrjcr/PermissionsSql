package com.nijiko.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    protected static final String parentGetText = "SELECT parentid FROM PrInheritance WHERE childid = ? ORDER BY parentorder;";

    protected static final String permAddText = "INSERT IGNORE INTO PrPermissions (entryid, permstring) VALUES (?,?);";
    protected static final String permRemText = "DELETE FROM PrPermissions WHERE entryid = ? AND permstring = ?;";
    protected static final String maxParentText = "SELECT MAX(parentorder) FROM PrInheritance WHERE childid = ?;";
    protected static final String parentAddText = "INSERT IGNORE INTO PrInheritance (childid, parentid, parentorder) VALUES (?,?,?);";
    protected static final String parentRemText = "DELETE FROM PrInheritance WHERE childid = ? AND parentid = ?;";

    protected static final String parentRemAllText = "DELETE FROM PrInheritance WHERE childid = ?;";

    protected static final String entryListText = "SELECT name, entryid FROM PrEntries WHERE worldid = ? AND type = ?;";
    protected static final String entryDelText = "DELETE FROM PrEntries WHERE worldid = ? AND entryid = ?;";

    protected static final String dataGetText = "SELECT data FROM PrData WHERE entryid = ? AND path = ?;";
    protected static final String dataModText = "REPLACE INTO PrData (data, entryid, path) VALUES (?,?,?);";
    protected static final String dataDelText = "DELETE FROM PrData WHERE entryid = ? AND path = ?;";

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
            List<Object[]> results = SqlStorage.runQuery(permGetText, new Object[] { id }, false, 1);
            if (results != null) {
                for (Object[] row : results) {
                    Object o = row[0];
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
            List<Object[]> results = SqlStorage.runQuery(parentGetText, new Object[] { uid }, false, 1);
            if (results != null) {
                for (Object[] row : results) {
                    Object o = row[0];
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
        SqlStorage.runUpdate(permAddText, new Object[] { uid, permission });
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
        SqlStorage.runUpdate(permRemText, new Object[] { uid, permission });
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
        int parentOrder = 0;
        List<Object[]> results = SqlStorage.runQuery(maxParentText, new Object[] { uid }, true, 1);
        if (results != null && !results.isEmpty()) {
            Object o = results.get(0)[0];
            if (o instanceof Integer) {
                parentOrder = (Integer) o;
            }
        }
        parentOrder++;
        SqlStorage.runUpdate(parentAddText, new Object[] { uid, gid, parentOrder });
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
        SqlStorage.runUpdate(parentRemText, new Object[] { uid, gid });
    }

    @Override
    public void setParents(String name, LinkedHashSet<GroupWorld> groupWs) {
        int uid;
        int[] gids = new int[groupWs.size()];
        Connection conn = null;
        try {
            uid = getId(name);
            int i = 0;
            for (GroupWorld gw : groupWs) {
                i++;
                gids[i] = SqlStorage.getEntry(gw.getWorld(), gw.getName(), true);
            }

            conn = SqlStorage.getConnection();
            conn.setAutoCommit(false);

            SqlStorage.runUpdate(conn, parentRemAllText, new Object[] { uid });

            Object[] pair = new Object[2];
            pair[0] = uid;

            for (int gid : gids) {
                pair[1] = gid;
                SqlStorage.runUpdate(conn, parentAddText, pair);
            }
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        } finally {
            if (conn != null)
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        // TODO
    }

    @Override
    public Set<String> getEntries() {
        if (idCache.isEmpty()) {
            List<Object[]> results = SqlStorage.runQuery(entryListText, new Object[] { worldId, (byte) (this.getType() == EntryType.GROUP ? 1 : 0) }, false, 1, 2);
            for (Object[] row : results) {
                Object oName = row[0];
                Object oId = row[1];
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
        int val = SqlStorage.runUpdate(entryDelText, new Object[] { worldId, id });
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
        List<Object[]> results = SqlStorage.runQuery(dataGetText, new Object[] { uid, path }, true, 1);
        for (Object[] row : results) {
            Object o = row[0];
            if (o instanceof String) {
                data = (String) o;
            }
        }
        return data;
    }

    @Override
    public Integer getInt(String name, String path) {
        String raw = getString(name, path);
        if (raw == null)
            return null;
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
        if (raw == null)
            return null;
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
        if (raw == null)
            return null;
        if (raw.equalsIgnoreCase("true")) {
            return true;
        } else if (raw.equalsIgnoreCase("false")) {
            return false;
        } else {
            return null;
        }
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
        SqlStorage.runUpdate(dataModText, new Object[] { szForm, uid, path });
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
        SqlStorage.runUpdate(dataDelText, new Object[] { uid, path });
    }

    public Integer getCachedId(String name) {
        return idCache.get(name);
    }

    protected abstract int getId(String name) throws SQLException;

}
