package com.nijiko.data;

import java.sql.SQLException;

import com.nijiko.permissions.EntryType;

public class SqlUserStorage extends SqlEntryStorage implements UserStorage {

    public SqlUserStorage(String world, int id) {
        super(world, id);
    }

    @Override
    public EntryType getType() {
        return EntryType.USER;
    }

    @Override
    protected int getId(String name) throws SQLException {
      int uid = -1;
      if (idCache.containsKey(name))
          uid = idCache.get(name);
      else {
          uid = SqlStorage.getEntry(world, name, false);
          idCache.put(name, uid);
      }
      return uid;
    }

}
