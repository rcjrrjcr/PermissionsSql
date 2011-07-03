package com.nijiko.data;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.nijiko.permissions.EntryType;

public class SqlGroupStorage extends SqlEntryStorage implements GroupStorage {

    private String defaultGroup = null;
    
    private static final String defGroupText = "SELECT defaultid FROM PrWorldBase WHERE worldid = ?;";
    private static final String trackListText = "SELECT trackname FROM PrTracks WHERE worldid = ?;";
    private static final String trackGetText = "SELECT PrWorlds.worldname, PrEntries.name FROM PrWorlds, PrEntries, PrTracks, PrTrackGroups WHERE PrTrackGroups.trackid = PrTracks.trackid AND PrTracks.worldid = ? AND PrTracks.trackname = ? AND PrEntries.entryid = PrTrackGroups.gid AND PrWorlds.worldid = PrEntries.worldid ORDER BY PrTrackGroups.groupOrder;";
        
    public SqlGroupStorage(String world, int id) {
        super(world, id);
    }

    @Override
    public EntryType getType() {
        return EntryType.GROUP;
    }

    @Override
    public boolean isDefault(String name) {
        if(defaultGroup != null) {
            return defaultGroup.equals(name);
        }
        List<Object[]> results = SqlStorage.runQuery(defGroupText, new Object[]{worldId}, true, 1);
        Iterator<Object[]> iter = results.iterator();
        if(iter.hasNext()) {
            Object def = iter.next()[0];
            if(def instanceof Integer) {
                int defId = (Integer) def;
                defaultGroup = SqlStorage.getEntryName(defId).name;
                return defaultGroup.equals(name);
            }
        }
        return false;
    }

    @Override
    public Set<String> getTracks() {
        List<Object[]> results = SqlStorage.runQuery(trackListText, new Object[]{worldId}, false, 1);
        Set<String> tracks = new LinkedHashSet<String>();
        for(Object[] arr : results) {
            Object o = arr[0];
            if(o instanceof String) {
                String s = (String) o;
                if(s.equals("deftrack"))
                    s = null;
                tracks.add(s);
            }
        }
        return tracks;
    }

    @Override
    public LinkedList<GroupWorld> getTrack(String track) {
        if(track == null) {
            track = "deftrack"; //Name of default SQL track
        }
        List<Object[]> results = SqlStorage.runQuery(trackGetText, new Object[]{worldId, track}, false, 1, 1);
        LinkedList<GroupWorld> trackGroups = new LinkedList<GroupWorld>();
        for(Object[] arr : results) {
            Object oWorld = arr[0];
            Object oName = arr[1];
            if(oWorld instanceof String && oName instanceof String) {                
                trackGroups.add(new GroupWorld((String)oWorld, (String)oName));
            }
        }
        return trackGroups;
    }

    @Override
    protected int getId(String name) throws SQLException {
        int gid = -1;
        if (idCache.containsKey(name))
            gid = idCache.get(name);
        else {
            gid = SqlStorage.getEntry(world, name, true);
            idCache.put(name, gid);
        }
        return gid;
    }
    
}
