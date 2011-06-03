package com.nijiko.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PreparedStatementPool {
    
    private Connection dbConn;
    private final String statement;
    
    private final BlockingQueue<PreparedStatement> pool;
    private final static long timeout = 100;
    
    private final ReadWriteLock rwl = new ReentrantReadWriteLock(false);
    public PreparedStatementPool(Connection dbConn, String statement, int max) {
        this.dbConn = dbConn;
        this.statement = statement;
        pool = new ArrayBlockingQueue<PreparedStatement>(max);
    }
    
    public void close() {
        rwl.writeLock().lock();
        try {
            Set<PreparedStatement> stmts = new HashSet<PreparedStatement>();
            pool.drainTo(stmts);
            for(PreparedStatement p : stmts) {
                try {
                    p.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }
    
    private void freeStatement(PreparedStatement p) {
        rwl.readLock().lock();
        try {
            boolean b = false;
            try {
                b = pool.offer(p, timeout, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {}
            if(!b)
                try {
                    p.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        } finally {
            rwl.readLock().unlock();
        }
    }
    public PreparedStatementWrapper getStatement() throws SQLException {
        PreparedStatementWrapper wrap = null;
        rwl.readLock().lock();
        try {
            PreparedStatement p = null;
            try {
                p = pool.poll(timeout, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {}
            if(p == null) {
                p = dbConn.prepareStatement(statement);
            }
            wrap = new PreparedStatementWrapper(p, this);
        } finally {
            rwl.readLock().unlock();
        }
        return wrap;
    }
    
    static class PreparedStatementWrapper {
        
        private final PreparedStatement p;
        private final PreparedStatementPool pool;
        private boolean valid;
        
        public PreparedStatementWrapper(PreparedStatement p, PreparedStatementPool pool) {
            this.p = p;
            this.pool = pool;
            valid = true;
        }

        public synchronized void close() {
            if(!valid) return; 
            pool.freeStatement(p);
            valid = false;
        }
        
        public PreparedStatement getStatement() {
            if(!valid) return null;
            return p;
        }
        
        @Override
        protected void finalize() {
            close();
        }
    }
}