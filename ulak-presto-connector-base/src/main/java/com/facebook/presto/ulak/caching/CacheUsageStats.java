package com.facebook.presto.ulak.caching;

import java.util.Date;

public class CacheUsageStats {
    private int used;
    private Date lastUsed;
    private int hash;

    public CacheUsageStats(int used, Date lastUsed, int hash) {
        this.used = used;
        this.lastUsed = lastUsed;
        this.hash = hash;
    }

    public int getUsed() {
        return used;
    }

    public void setUsed(int used) {
        this.used = used;
    }
    public int getQueryHash() {
        return hash;
    }

    public void setQueryHash(int hash) {
        this.hash = hash;
    }
    public Date getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(Date lastUsed) {
        this.lastUsed = lastUsed;
    }

    @Override
    public int hashCode() {
        int result = (hash ^ (hash >>> 32));
        result = 31 * result + used;
        result = 31 * result + lastUsed.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof CacheUsageStats))
            return false;
        CacheUsageStats pn = (CacheUsageStats)o;
        return pn.hash == hash && pn.used == used
                && pn.lastUsed.equals(lastUsed);
    }
}
