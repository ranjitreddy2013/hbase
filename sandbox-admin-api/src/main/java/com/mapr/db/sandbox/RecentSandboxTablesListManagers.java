package com.mapr.db.sandbox;

import com.google.common.collect.Maps;

import java.util.Map;

public class RecentSandboxTablesListManagers {
    private static Map<String, RecentSandboxTablesListManager> registry = Maps.newHashMap();

    public synchronized static RecentSandboxTablesListManager getRecentSandboxTablesListManagerForUser(String user) {
        if (!registry.containsKey(user)) {
            registry.put(user, new RecentSandboxTablesListManager(user));
        }
        return registry.get(user);
    }
}