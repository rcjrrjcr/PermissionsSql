package com.nijiko.data;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

public class ConnectionPool {
    private final BlockingQueue<WrappedConnection> queue;
    private final DataSource dbSource;
    private final Thread reaperThread = new Thread(new Reaper());
    private volatile boolean isShutdown = false;
    private static final AtomicInteger maxWrapCount = new AtomicInteger(0); // Used for testing purposes.
    private static final AtomicInteger currentWrapCount = new AtomicInteger(0); // Used for testing purposes.

    private ConnectionPool(DataSource dbSource, int maxConnections) {
        this.dbSource = dbSource;
        this.queue = new ArrayBlockingQueue<WrappedConnection>(maxConnections);
    }
    
    public static ConnectionPool newInstance(DataSource dbSource, int maxConnections) {
        ConnectionPool cp = new ConnectionPool(dbSource, maxConnections);
        cp.reaperThread.start();
        return cp;
    }

    public Connection getConnection() throws SQLException {
        WrappedConnection wrapConn;
        while (true) {
            if(isShutdown)
                return null;
            wrapConn = queue.poll();

            if (wrapConn == null) {
//                System.out.println("Creating new connection");
                wrapConn = this.createNewConnection();
            } else {
                if (!isValid(wrapConn)) {
//                    System.out.println("Invalid connection detected");
                    continue;
                }
            }
            
            if(wrapConn.lease()) {
//                System.out.println("Connection leased");
                break;
            }
        }
        return wrapConn;
    }

    private boolean isValid(WrappedConnection wrapConn) {
        try {
            return wrapConn.isValid(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private WrappedConnection createNewConnection() throws SQLException {
        WrappedConnection w = new WrappedConnection(dbSource.getConnection());
        return w;
    }

    private void returnToPool(WrappedConnection wrapConn) {
        if(!wrapConn.expireLease())
            return;
        if (!isShutdown && isValid(wrapConn) && queue.offer(wrapConn)) {
//            System.out.println("Wrapper returned to pool");
            return;
        } else {
//            System.out.println("Offer failed. Destroying wrapper");
            wrapConn.destroy();
        }
    }
    
    //TODO: Destroy active wrappers too
    public void closeAll() {
//        System.out.println("Shutting down");
        isShutdown = true;
        reaperThread.interrupt();
        List<WrappedConnection> wrappers = new ArrayList<WrappedConnection>(queue.size());
        queue.drainTo(wrappers);
        for(WrappedConnection wrap : wrappers) {
//            System.out.println("Shutting down connection");
            wrap.destroy();
        }
    }

    private class WrappedConnection implements Connection {

        private final Connection conn;
        private AtomicReference<Thread> owner = new AtomicReference<Thread>();
        private long timeStamp = System.currentTimeMillis();

        WrappedConnection(Connection conn) {
            this.conn = conn;

            for (;;) {
                int max = ConnectionPool.maxWrapCount.get();
                int current = ConnectionPool.currentWrapCount.incrementAndGet();
                if (current > max) {
                    if (ConnectionPool.maxWrapCount.compareAndSet(max, current))
                        break;
                }
            }
        }

        protected void destroy() {
//            System.out.println("Destroying wrapper");
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ConnectionPool.currentWrapCount.decrementAndGet();
        }

        public boolean lease() {
            if (owner.compareAndSet(null, Thread.currentThread())) {
                timeStamp = System.currentTimeMillis();
                return true;
            } else
                return false;
        }

        public long getLastUse() {
            return timeStamp;
        }

        protected boolean expireLease() {
            return owner.compareAndSet(Thread.currentThread(), null);
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            try {
                return conn.isValid(timeout);
            } catch (AbstractMethodError stupidSqliteHowTheHeckDidYouCompilePartiallyDefinedClass) {
                return true;
            }
        }

        @Override
        public void close() throws SQLException {
//            System.out.println("Closing " + this);
            returnToPool(this);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return conn.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return conn.isWrapperFor(iface);
        }

        @Override
        public Statement createStatement() throws SQLException {
            return conn.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return conn.prepareStatement(sql);
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return conn.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return conn.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            conn.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return conn.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            conn.commit();
        }

        @Override
        public void rollback() throws SQLException {
            conn.rollback();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return conn.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return conn.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            conn.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return conn.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            conn.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return conn.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            conn.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return conn.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return conn.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            conn.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return conn.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return conn.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            conn.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            conn.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return conn.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return conn.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return conn.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            conn.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            conn.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return conn.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return conn.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return conn.prepareStatement(sql, columnNames);
        }

        @Override
        public Clob createClob() throws SQLException {
            return conn.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return conn.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return conn.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return conn.createSQLXML();
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            conn.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            conn.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return conn.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return conn.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return conn.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return conn.createStruct(typeName, attributes);
        }

    }

    private class Reaper implements Runnable {

        private static final long delay = 5L * 60L * 1000L;
        private static final long timeout = 30L * 60L * 1000L;

        @Override
        public void run() {
            WrappedConnection wrapConn;
            for (;;) {
                if(isShutdown) {
//                    System.out.println("Stopping due to shutdown");
                    return;
                }
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
//                    System.out.println("Interrupted while sleeping");
                    return;
                }
                
//                System.out.println("Starting reaping.");
                
                for (;;) {
                    try {
                        wrapConn = queue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
//                        System.out.println("Interrupted while polling queue");
                        return;
                    }
                    
                    if (wrapConn == null) {
                        break;
                    } else {
                        if (isValid(wrapConn) && (wrapConn.getLastUse() + timeout) < System.currentTimeMillis())
                            queue.add(wrapConn);
                        else
                            wrapConn.destroy();
                    }
                }
                
            }
        }

    }
}
