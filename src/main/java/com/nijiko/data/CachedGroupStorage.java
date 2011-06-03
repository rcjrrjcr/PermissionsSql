package com.nijiko.data;

import java.util.LinkedList;
import java.util.Set;

public class CachedGroupStorage extends CachedStorage implements GroupStorage {

    private final GroupStorage wrapped;
    
    public CachedGroupStorage(GroupStorage wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public boolean isDefault(String name) {
        return wrapped.isDefault(name);
    }

    @Override
    public Set<String> getTracks() {
        return wrapped.getTracks();
    }

    @Override
    public LinkedList<GroupWorld> getTrack(String track) {
        return wrapped.getTrack(track);
    }

    @Override
    protected Storage getWrapped() {
        return wrapped;
    }
    
}
