package com.mapr.db.sandbox;

import com.google.common.collect.Sets;
import com.mapr.db.sandbox.ProxyManager.ProxyInfo;
import com.mapr.fs.MapRFileSystem;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ProxyManagerTest {
    MapRFileSystem mfs = mock(MapRFileSystem.class);
    ProxyManager pm = new ProxyManager(mfs);

    @Test(expected=SandboxException.class)
    public void testProxySelectionWithNoProxies() throws SandboxException {
        pm.pickBestProxy(Sets.<ProxyInfo>newTreeSet());
    }

    @Test(expected=SandboxException.class)
    public void testProxySelectionWithFullProxies() throws SandboxException {
        Set<ProxyInfo> proxies = Sets.<ProxyInfo>newTreeSet();

        ProxyInfo p1 = new ProxyInfo();
        p1.proxyFid = "p1";
        p1.upstreamCount = 64;

        ProxyInfo p2 = new ProxyInfo();
        p2.proxyFid = "p2";
        p2.upstreamCount = 64;

        ProxyInfo p3 = new ProxyInfo();
        p3.proxyFid = "p3";
        p3.upstreamCount = 64;

        proxies.add(p1);
        proxies.add(p2);
        proxies.add(p3);

        pm.pickBestProxy(proxies);
    }

    @Test
    public void testProxySelection() throws SandboxException {
        Set<ProxyInfo> proxies = Sets.<ProxyInfo>newTreeSet();

        ProxyInfo p1 = new ProxyInfo();
        p1.proxyFid = "p1";
        p1.upstreamCount = 2;

        ProxyInfo p2 = new ProxyInfo();
        p2.proxyFid = "p2";
        p2.upstreamCount = 5;

        ProxyInfo p3 = new ProxyInfo();
        p3.proxyFid = "p3";
        p3.upstreamCount = 1;

        proxies.add(p1);
        proxies.add(p2);
        proxies.add(p3);

        ProxyInfo selectedProxy = pm.pickBestProxy(proxies);
        assertEquals("p3", selectedProxy.proxyFid);
        assertEquals(1, selectedProxy.upstreamCount);
    }
}