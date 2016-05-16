package com.mapr.rest;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by arod on 14/01/2016.
 */
public class MapRRestClientTest {
// TODO do not commit
    @Test
    public void testVolumeInfo() throws Exception {
        MapRRestClient client = new MapRRestClient("localhost:8443", "mapr", "mapr");
        client.volumeInfo(new Path("/exp1"));
    }
}