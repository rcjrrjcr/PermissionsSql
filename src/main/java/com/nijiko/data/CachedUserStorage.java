package com.nijiko.data;

public class CachedUserStorage extends CachedStorage implements UserStorage {
    private final UserStorage wrapped;

    public CachedUserStorage(UserStorage wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    protected Storage getWrapped() {
        return wrapped;
    }

}
