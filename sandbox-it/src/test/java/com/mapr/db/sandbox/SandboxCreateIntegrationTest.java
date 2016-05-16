package com.mapr.db.sandbox;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

public class SandboxCreateIntegrationTest extends BaseSandboxIntegrationTest {
	@Test
	public void testRecentSandboxList() throws IOException {
		final List<String> globalList = sandboxAdmin.listRecent(null);
		final List<String> originalList = sandboxAdmin
				.listRecent(originalTablePath);
		assertTrue(globalList.contains(sandboxTablePath));
		assertTrue(originalList.contains(sandboxTablePath));
		assertEquals("original's sandbox list wrong size", 1,
				originalList.size());
	}

}
