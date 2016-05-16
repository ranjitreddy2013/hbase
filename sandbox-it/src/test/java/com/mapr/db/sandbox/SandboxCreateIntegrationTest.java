package com.mapr.db.sandbox;

import org.apache.hadoop.fs.Path;
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
	
	@Test(expected = SandboxException.class)
	public void testCreateDupSandbox() throws IOException, SandboxException {
		sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);
	}

	@Test(expected = SandboxException.class)
	public void testCreateSandboxWithExistingFilename() throws IOException,
			SandboxException {
		Path sandboxPath = new Path(sandboxTablePath);
		Path someFile = new Path(sandboxPath.getParent(), "someFile");
		fs.create(someFile);
		sandboxAdmin.createSandbox(someFile.toString(), originalTablePath);
	}
}
