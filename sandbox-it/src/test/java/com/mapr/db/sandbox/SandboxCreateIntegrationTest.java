package com.mapr.db.sandbox;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;


import java.io.IOException;
import java.util.List;

public class SandboxCreateIntegrationTest extends BaseSandboxIntegrationTest {
	@Test
	public void testRecentSandboxList() {
		final List<String> recentList = sandboxAdmin.listRecent();
		assertTrue(recentList.contains(sandboxTablePath));
	}

}
