package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Manages the tables list
 */
public class SandboxTablesListManager {
	private static final Logger LOG = Logger
			.getLogger(SandboxTablesListManager.class);

	private static SandboxTablesListManager GLOBAL_INSTANCE;
	private static Map<String, SandboxTablesListManager> registry = Maps
			.newHashMap();

	public static final String SBOX_TABLE_LIST_PREFIX_FORMAT = "/user/%s";
	public static final String GLOBAL_SANDBOX_TABLES_LIST_PATH = ".sandbox_tables";
	public static final String ORIGINAL_SBOX_LIST_FILENAME_FORMAT = ".sandbox_list%s"; // original
																						// fid

	private final Path listFilePath;
	private final MapRFileSystem fs;
	private final String ownerUsername;

	public static final String CF_NAME = "cf";
	private final byte[] CF = CF_NAME.getBytes();
	private static Configuration conf;
	private static HTable sandboxListTable;
	private static final String ROWKEY_FORMAT = "%s,%s";

	public SandboxTablesListManager(MapRFileSystem fs, Path listFilePath,
			String ownerUsername, String sandboxListTablePath) throws IOException {
		this.fs = fs;
		this.listFilePath = listFilePath;
		this.ownerUsername = ownerUsername;
		conf = new Configuration();
		this.sandboxListTable = new HTable(conf, sandboxListTablePath);
	}

	/*
	 * Store sandbox information as each sandbox is created.
	 * 
	 * @param productionTablePath production or original table 
	 * @param sandboxPath sandbox table path for the production table
	 * @param user user that owns the sandbox
	 */

	public void insertSandboxInInfoTable(String productionTablePath,
			String sandboxPath, String user) throws InterruptedIOException,
			RetriesExhaustedWithDetailsException {
		byte[] rowId = String.format(ROWKEY_FORMAT, productionTablePath, user)
				.getBytes();
		Put put = new Put(rowId);
		put.add(CF, sandboxPath.getBytes(), "1".getBytes());
		sandboxListTable.put(put);
		sandboxListTable.flushCommits();
	}

	/*
	 * Delete sandbox information as sandbox is pushed/deleted.
	 * 
	 * @param productionTablePath - production table path
	 * @param sandboxPath - sandbox table path
	 * @param user - user deleting the sandbox 
	 * 
	 */
	public void deleteSandboxFromInfoTable(String productionTablePath,
			String sandboxPath, String user) throws IOException {
		byte[] rowId = String.format(ROWKEY_FORMAT, productionTablePath, user)
				.getBytes();
		Delete del = new Delete(rowId);
		del.deleteColumn(CF, sandboxPath.getBytes());
		sandboxListTable.delete(del);
		sandboxListTable.flushCommits();
	}

	/*
	 * Return sandboxes for the given criteria. * If production table path is
	 * empty and user is null, uses start row key and end row key. * If
	 * production table path is NOT empty and user is null, fetches all the
	 * sandboxes for the production table * If both production table and user is
	 * passed, fetches sandboxes created by the user for the production table.
	 * 
	 * @param productionTablePath 
	 * @user  user requesting the sandboxes list
	 * @limit number of sandboxes to fetch
	 * @startRowKey - used as the start rowkey
	 * @endRowKey  - used as the stop rowkey
	 */

	public List<String> getAllSandboxes(String productionTablePath,
			String user, long limit, byte[] startRowKey, byte[] endRowKey)
			throws IOException {
		ResultScanner rs = null;
		List<String> sbList = new ArrayList<String>();

		final Scan scan = new Scan();
		byte[] startRowKeyByteArray = startRowKey;
		byte[] endRowKeyByteArray = endRowKey;

		if (StringUtils.isNotEmpty(productionTablePath)
				&& StringUtils.isNotEmpty(user)) {
			String rowkey = String.format(ROWKEY_FORMAT, productionTablePath,
					user);
			startRowKeyByteArray = rowkey.getBytes();
			endRowKeyByteArray = rowkey.getBytes();
		}

		if (StringUtils.isNotEmpty(productionTablePath)
				&& StringUtils.isEmpty(user)) {
			startRowKeyByteArray = productionTablePath.getBytes();
			endRowKeyByteArray = (productionTablePath + "ZZZ").getBytes();
		}

		scan.setStartRow(startRowKeyByteArray);
		scan.setStopRow(endRowKeyByteArray);

		PageFilter pageFilter = new PageFilter(limit);
		FilterList filterList = new FilterList();
		filterList.addFilter(pageFilter);
		scan.setFilter(filterList);

		rs = sandboxListTable.getScanner(scan);
		mapResultSet(rs, sbList);
		return sbList;
	}

	/*
	 * Return all sandboxes for the given user
	 * 
	 * @user - user requesting sandboxes
	 */

	public List<String> getAllSandboxesForUser(String user) throws IOException {
		ResultScanner rs = null;

		final Scan scan = new Scan();

		if (StringUtils.isNotEmpty(user)) {
			Filter rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(user));
			FilterList filterList = new FilterList();
			filterList.addFilter(rowKeyFilter);
			scan.setFilter(filterList);
		}

		rs = sandboxListTable.getScanner(scan);

		List<String> sbList = new ArrayList<String>();
		mapResultSet(rs, sbList);
		return sbList;
	}

	/*
	 * Uses the result set to map it to list
	 */
	private void mapResultSet(ResultScanner rs, List<String> sandboxList) {
		for (Result result : rs) {
			for (Cell kv : result.rawCells()) {
				if (SandboxTablesListManager.CF_NAME.equalsIgnoreCase(Bytes
						.toString(CellUtil.cloneFamily(kv)))) {
					byte[] colQualifier = CellUtil.cloneQualifier(kv);

					if (colQualifier != null) {
						sandboxList.add(Bytes.toString(colQualifier));
					}
				}
			}
		}
	}

	/**
	 * Move up the
	 * 
	 * @param mostRecentTable
	 *            to the top of the recent tables list, if it exists. Else, add
	 *            it to the top of the list.
	 *
	 * @param mostRecentTable
	 */
	public synchronized void moveToTop(String mostRecentTable) {
		List<String> recentTables = getListFromFile();
		if (recentTables.indexOf(mostRecentTable) != -1) {
			recentTables.remove(mostRecentTable);
		}
		recentTables.add(0, mostRecentTable);
		this.writeListToFile(recentTables);
	}

	/**
	 * Deletes
	 * 
	 * @param tablePath
	 *            from the recent tables list.
	 *
	 * @param tablePath
	 */
	public synchronized void delete(String tablePath) {
		List<String> recentTablesList = getListFromFile();
		if (recentTablesList.remove(tablePath)) {
			writeListToFile(recentTablesList);
		}
	}

	public void deleteIfNotExist(String tablePath, MapRFileSystem mfs) {
		try {
			if (!mfs.exists(new Path(tablePath))
					|| !mfs.getMapRFileStatus(new Path(tablePath)).isTable()) {
				this.delete(tablePath);
			}
		} catch (IOException e) {
			// ignore the exception
		}
	}

	public List<String> getListFromFile() {
		List<String> tablePaths = Lists.newArrayList();
		try {
			if (fs.isFile(listFilePath)) {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(listFilePath)));
				String path;
				while ((path = reader.readLine()) != null) {
					tablePaths.add(path);
				}
				reader.close();
			}
		} catch (Exception e) {
			LOG.error(e);
		}
		return tablePaths;
	}

	private void writeListToFile(List<String> recentTablesList) {
		try {
			// First write to a temp file...
			String tempFilePath = listFilePath.toString()
					+ new Random().nextInt();
			Path tmpFile = new Path(tempFilePath);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					fs.create(tmpFile, true).getWrappedStream()));
			for (String tablePath : recentTablesList) {
				writer.write(tablePath, 0, tablePath.length());
				writer.newLine();
			}
			writer.close();

			// set the permissions and then rename
			fs.setPermission(tmpFile,
					FsPermission.createImmutable((short) 00700));

			if (ownerUsername != null) {
				fs.setOwner(tmpFile, ownerUsername, null);
			}

			fs.rename(tmpFile, listFilePath);
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	public static SandboxTablesListManager global(MapRFileSystem fs,
			String username, String sandboxListTablePath) throws IOException {
		if (GLOBAL_INSTANCE == null) {
			Path sboxGlobalListFile = new Path(String.format("%s/%s",
					String.format(SBOX_TABLE_LIST_PREFIX_FORMAT, username),
					GLOBAL_SANDBOX_TABLES_LIST_PATH));
			GLOBAL_INSTANCE = new SandboxTablesListManager(fs,
					sboxGlobalListFile, username, sandboxListTablePath);
		}

		return GLOBAL_INSTANCE;
	}

}
