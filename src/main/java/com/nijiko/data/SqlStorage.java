package com.nijiko.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.sqlite.SQLiteDataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public abstract class SqlStorage {

    private static Dbms dbms;
    private static DataSource dbSource;
    private static boolean init = false;
    private static HashMap<String, SqlUserStorage> userStores = new HashMap<String, SqlUserStorage>();
    private static Map<String, SqlGroupStorage> groupStores = new HashMap<String, SqlGroupStorage>();
    private static Map<String, Integer> worldMap = new HashMap<String, Integer>();
    private static final List<String> create = new ArrayList<String>(8);
    static final String getWorld = "SELECT worldid FROM PrWorlds WHERE worldname = ?;";
    static final String getEntry = "SELECT entryid FROM PrEntries WHERE worldid = ? AND type = ? AND name = ?;";
    static final String createWorld = "INSERT IGNORE INTO PrWorlds (worldname) VALUES (?);";
    static final String createEntry = "INSERT IGNORE INTO PrEntries (worldid,type,name) VALUES (?,?,?);";
    static final String getWorldName = "SELECT worldname FROM PrWorlds WHERE worldid = ?;";
    static final String getEntryName = "SELECT name, worldid FROM PrEntries WHERE entryid = ?;";

    // XXX: Connection objects aren't really thread safe. Either use connection pools or ThreadLocal
    private static ConnectionPool pool;

    static {
        create.add("CREATE TABLE IF NOT EXISTS PrWorlds (" + " worldid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " worldname VARCHAR(32) NOT NULL UNIQUE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrEntries (" + " entryid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " name VARCHAR(32) NOT NULL," + " worldid INTEGER NOT NULL," + " type TINYINT NOT NULL," + " CONSTRAINT NameWorld UNIQUE (name, worldid, type)," + " ENTRYINDEX" + " FOREIGN KEY(worldid) REFERENCES PrWorlds(worldid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrPermissions (" + " permid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " permstring VARCHAR(64) NOT NULL," + " entryid INTEGER NOT NULL," + " CONSTRAINT PrEntryPerm UNIQUE (entryid, permstring)," + " FOREIGN KEY(entryid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrInheritance (" + " uinheritid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " childid INTEGER NOT NULL," + " parentid INTEGER NOT NULL," + " parentorder INTEGER NOT NULL," + " CONSTRAINT PrParent UNIQUE (childid, parentid)," + " CONSTRAINT PrOrderedInheritance UNIQUE (childid, parentorder)," + " CONSTRAINT PrNoSelfInherit CHECK (childid <> parentid)," + " FOREIGN KEY(childid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE," + " FOREIGN KEY(parentid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrWorldBase (" + " worldid INTEGER NOT NULL," + " defaultid INTEGER," + " FOREIGN KEY(worldid) REFERENCES PrWorlds(worldid) ON DELETE CASCADE ON UPDATE CASCADE," + " FOREIGN KEY(defaultid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrData (" + " dataid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " entryid INTEGER NOT NULL ," + " path VARCHAR(64) NOT NULL," + " data VARCHAR(64) NOT NULL," + " CONSTRAINT PrDataUnique UNIQUE (entryid, path)," + " FOREIGN KEY(entryid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrTracks (" + " trackid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " trackname VARCHAR(64) NOT NULL UNIQUE," + " worldid INTEGER NOT NULL," + " CONSTRAINT TracksUnique UNIQUE (trackid, worldid)," + " FOREIGN KEY(worldid) REFERENCES PrWorlds(worldid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
        create.add("CREATE TABLE IF NOT EXISTS PrTrackGroups (" + " trackgroupid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," + " trackid INTEGER NOT NULL," + " gid INTEGER NOT NULL," + " groupOrder INTEGER NOT NULL," + " CONSTRAINT TrackGroupsUnique UNIQUE (trackname, gid)," + " FOREIGN KEY(trackid) REFERENCES PrTracks(trackid) ON DELETE CASCADE ON UPDATE CASCADE," + " FOREIGN KEY(gid) REFERENCES PrEntries(entryid) ON DELETE CASCADE ON UPDATE CASCADE" + ")");
    }

    static Dbms getDbms() {
        return dbms;
    }

    public synchronized static void init(String dbmsName, String uri, String username, String password, int reloadDelay) throws Exception {
        if (init) {
            return;
        }

        System.out.println("[Permissions] Initializing Permissions 3 SQL interface.");
        // SqlStorage.reloadDelay = reloadDelay;
        try {
            dbms = Dbms.valueOf(dbmsName);
        } catch (IllegalArgumentException e) {
            System.err.println("[Permissions] Error occurred while selecting permissions config DBMS. Reverting to SQLite.");
            dbms = Dbms.SQLITE;
        }
        try {
            Class.forName(dbms.getDriver());
        } catch (ClassNotFoundException e) {
            throw new Exception("[Permissions] Unable to load SQL driver!", e);
        }
        dbSource = dbms.getSource(username, password, uri);
        pool = ConnectionPool.newInstance(dbSource, 5);
        verifyAndCreateTables();
        init = true;
        clearWorldCache();
    }

    public synchronized static void clearWorldCache() // Used for periodic cache
    // flush
    {
        if (init)
            worldMap.clear();
    }

    private static void verifyAndCreateTables() throws SQLException {
        Connection dbConn = null;
        Statement s = null;
        try {
            dbConn = SqlStorage.getConnection();
            s = dbConn.createStatement();
            if (dbms == Dbms.SQLITE) {
                s.execute("PRAGMA foreign_keys = ON;");
            }
            // TODO: Verify stuff
            String engine = dbms.equals(Dbms.MYSQL) ? " ENGINE = InnoDB;" : ";";
            for (String state : create) {
                if (dbms == Dbms.MYSQL) {
                    state = state.replace("AUTOINCREMENT", "AUTO_INCREMENT");
                    state = state.replace(" ENTRYINDEX", " INDEX pr_entryname_index(name),");
                } else {
                    state = state.replace(" ENTRYINDEX", "");
                }
                s.executeUpdate(state + engine);
            }
            if (dbms != Dbms.MYSQL) {
                s.executeUpdate("CREATE INDEX IF NOT EXISTS pr_entry_index ON PrEntries(name);");
            }
        } finally {
            if (s != null)
                s.close();
            if (dbConn != null)
                dbConn.close();
        }
    }

    static DataSource getSource() {
        return dbSource;
    }

    static int getWorld(String name) {
        if (worldMap.containsKey(name)) {
            // System.out.println(worldMap.get(name));
            return worldMap.get(name);
        }
        Object[] params = new Object[] { name };
        List<Object[]> results = runQuery(getWorld, params, true, 1);
        if (results.isEmpty()) {
            System.out.println("[Permissions] Creating world '" + name + "'.");
            runUpdate(createWorld, params);
            results = runQuery(getWorld, params, true, 1);
        }
        int id = -1;
        Iterator<Object[]> iter = results.iterator();
        // System.out.println(results);
        if (iter.hasNext()) {
            Object o = iter.next()[0];
            if (o instanceof Integer) {
                id = (Integer) o;
            }
        }
        worldMap.put(name, id);
        return id;
    }

    static int getEntry(String world, String name, boolean isGroup) {
        SqlEntryStorage ses = isGroup ? getGroupStorage(world) : getUserStorage(world);
        Integer cachedId = ses.getCachedId(name);
        if (cachedId != null) {
            return cachedId;
        }
        int worldid = getWorld(world);
        Object[] params = new Object[] { worldid, (byte) (isGroup ? 1 : 0), name };
        List<Object[]> results = runQuery(getEntry, params, true, 1);
        if (results.isEmpty()) {
            System.out.println("[Permissions] Creating " + (isGroup ? "group" : "user") + " '" + name + "' in world '" + world + "'.");
            runUpdate(createEntry, params);
            results = runQuery(getEntry, params, true, 1);
        }
        int id = -1;
        Iterator<Object[]> iter = results.iterator();
        if (iter.hasNext()) {
            Object o = iter.next()[0];
            if (o instanceof Integer) {
                id = (Integer) o;
            }
        }
        return id;
    }

    static String getWorldName(int id) {
        List<Object[]> results = runQuery(getWorldName, new Object[] { id }, true, 1);
        Iterator<Object[]> iter = results.iterator();
        String name = "Error";
        if (iter.hasNext()) {
            Object o = iter.next()[0];
            if (o instanceof String)
                name = (String) o;
        }
        worldMap.put(name, id);
        return name;
    }

    static NameWorldId getEntryName(int id) {
        List<Object[]> results = runQuery(getEntryName, new Object[] { id }, true, 1, 2);
        Iterator<Object[]> iter = results.iterator();
        NameWorldId nw = new NameWorldId();
        if (!iter.hasNext()) {
            nw.name = "Error";
            nw.worldid = -1;
            return nw;
        }
        Object[] row = iter.next();
        Object oName = row[0]; //Offset by -1 due to zero-indexed array
        Object oWId = row[1];
        String name = null;
        int worldid = -1;
        if (oName instanceof String && oWId instanceof Integer) {
            name = (String) oName;
            worldid = (Integer) oWId;
        }
        nw.name = name;
        nw.worldid = worldid;
        return nw;
    }

    static SqlUserStorage getUserStorage(String world) {
        if (userStores.containsKey(world)) {
            return userStores.get(world);
        }
        SqlUserStorage sus = new SqlUserStorage(world, getWorld(world));
        userStores.put(sus.getWorld(), sus);
        return sus;
    }

    static SqlGroupStorage getGroupStorage(String world) {
        if (groupStores.containsKey(world)) {
            return groupStores.get(world);
        }
        SqlGroupStorage sgs = new SqlGroupStorage(world, getWorld(world));
        groupStores.put(sgs.getWorld(), sgs);
        return sgs;
    }

    public synchronized static void closeAll() {
        if (init) {
            userStores.clear();
            groupStores.clear();
            worldMap.clear();
            pool.closeAll();
            dbSource = null;
            init = false;
        }
    }

    static Connection getConnection() throws SQLException {
        return pool.getConnection();
    }

    public static class NameWorldId {
        public int worldid;
        public String name;
    }

    static List<Object[]> runQuery(String statement, Object[] params, boolean single, int... dataCols) {
        Connection dbConn;
        try {
            dbConn = getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return new ArrayList<Object[]>();
        }
        List<Object[]> data = runQuery(dbConn, statement, params, single, dataCols);
        try {
            if (dbConn != null)
                dbConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return data;
    }

    static List<Object[]> runQuery(Connection dbConn, String statement, Object[] params, boolean single, int... dataCols) {
        if (dataCols == null || dataCols.length == 0) {
            return null;
        }
        List<Object[]> results = new LinkedList<Object[]>();

        PreparedStatement stmt = null;
        try {
            stmt = dbConn.prepareStatement(statement);
            fillStatement(stmt, params);
            if (stmt.execute()) {
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    Object[] row = new Object[dataCols.length];
                    for (int i = 0; i < dataCols.length; i++) {
                        row[i] = rs.getObject(dataCols[i]);
                        if (dbms == Dbms.MYSQL && rs.isClosed())
                            break;
                    }
                    results.add(row);
                    if (single)
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return results;
    }

    static int runUpdate(String statement, Object[] params) {
        Connection dbConn;
        try {
            dbConn = getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        int data = runUpdate(dbConn, statement, params);
        try {
            if (dbConn != null)
                dbConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return data;
        
    }
    
    static int runUpdate(Connection dbConn, String statement, Object[] params) {
        String sql = dbms == Dbms.SQLITE ? statement.replace("INSERT IGNORE", "INSERT OR IGNORE") : statement;
        int result = -1;

        PreparedStatement stmt = null;
        try {
            stmt = dbConn.prepareStatement(sql);
            fillStatement(stmt, params);
            stmt.execute();
            result = stmt.getUpdateCount();
        } catch (SQLException e) {
            e.printStackTrace();
            result = -1;
        } finally {
            if (stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return result;
    }

    static void fillStatement(PreparedStatement stmt, Object[] params) throws SQLException {
        stmt.clearParameters();
        if (params == null)
            return;
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param != null) {
                stmt.setObject(i + 1, param);
            }
        }
    }
}

enum Dbms {

    SQLITE("org.sqlite.JDBC"), MYSQL("com.mysql.jdbc.Driver");
    private final String driver;

    Dbms(String driverClass) {
        this.driver = driverClass;
    }

    public String getDriver() {
        return driver;
    }

    public DataSource getSource(String username, String password, String url) {
        switch (this) {
        case MYSQL:
            MysqlDataSource mds = new MysqlDataSource();
            mds.setUser(username);
            mds.setPassword(password);
            mds.setUrl(url);
            mds.setCachePreparedStatements(true);
            mds.setPreparedStatementCacheSize(21);
            mds.setUseServerPrepStmts(true);
            // mds.setPreparedStatementCacheSqlLimit(308);
            return mds;
        default:
        case SQLITE:
            SQLiteDataSource sds = new SQLiteDataSource();
            sds.setUrl(url);
            sds.setEnforceForeinKeys(true);
            return sds;
        }
    }
}
