package com.mapr.rest;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.mapr.db.sandbox.SandboxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.TextUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MapRRestClient {
    final String username;
    private final String baseUrl;

    final HttpClient client;

    public MapRRestClient(String[]hostnames, String username, String password) throws SandboxException {
        this(new Configuration(), hostnames, username, password);
    }

    public MapRRestClient(Configuration conf, String[]hostnames, String username, String password) throws SandboxException {
        // TODO make sure we handle multiple endpoints
        this.baseUrl = String.format("https://%s/rest", hostnames[0]);
        this.username = username;

        // TODO convert to singleton?
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
        provider.setCredentials(AuthScope.ANY, credentials);

        try {
            SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(new TrustSelfSignedStrategy()).build();
            SSLEngine engine = sslContext.createSSLEngine();

            final List<String> enabledCiphersList = Lists.newArrayList(engine.getEnabledCipherSuites());
            enabledCiphersList.removeAll(Lists.newArrayList(conf.getStrings("hadoop.ssl.exclude.cipher.suites")));

            final String[] enabledCipherSuits = new String[enabledCiphersList.size()];
            enabledCiphersList.toArray(enabledCipherSuits);

            client = HttpClientBuilder.create()
                .setSSLSocketFactory(new SSLConnectionSocketFactory(
                        sslContext,
                        split(System.getProperty("https.protocols",
                                Joiner.on(",").join(engine.getEnabledProtocols()))),
                        enabledCipherSuits,
                        new NoopHostnameVerifier()))
                .setDefaultCredentialsProvider(provider)
                .build();
        } catch (NoSuchAlgorithmException e) {
            throw new SandboxException("Could not create https client", e);
        } catch (KeyManagementException e) {
            throw new SandboxException("Could not create https client", e);
        } catch (KeyStoreException e) {
            throw new SandboxException("Could not create https client", e);
        }
    }

    public void testCredentials() throws SandboxException {
        HttpResponse response = null;
        try {
            response = client.execute(new HttpGet(baseUrl));
        } catch (IOException e) {
            throw new SandboxException(String.format("Could not test credentials against url = %s", baseUrl), e);
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 401) {
            throw new SandboxException("User/password invalid", null);
        }
    }


    public boolean waitForState(String confirmUrlPath, boolean ignoreErrors, String column, String expectedValue, long timeout) throws SandboxException {
        long startTime = System.currentTimeMillis(), endTime;

        do {
            try {
                JSONObject result = callCommand(confirmUrlPath, ignoreErrors);

                JSONArray data = result.has("data") ? result.getJSONArray("data") : null;

                if (data != null) {
                    String returnedValue = data.getJSONObject(0).getString(column);

                    if (expectedValue.equals(returnedValue)) {
                        return true;
                    }

                    Thread.sleep(1000L);
                }
            } catch (InterruptedException e) {
                throw new SandboxException(String.format("Error waiting for state %s=%s (url = %s)",
                        column, expectedValue, confirmUrlPath), e);
            } catch (JSONException e) {
                throw new SandboxException(String.format("Error parsing state %s=%s (url = %s)",
                        column, expectedValue, confirmUrlPath), e);
            }
            endTime = System.currentTimeMillis();
        } while(endTime - startTime < timeout);

        return false;
    }

    public JSONObject callCommand(String urlPath, boolean ignoreErrors) throws SandboxException {
        final String url = baseUrl + urlPath;
        // TODO implement timeouts
        HttpResponse response = null;
        try {
            response = client.execute(new HttpGet(url));
        } catch (IOException e) {
            throw new SandboxException("Error performing REST API request", e);
        }

        JSONObject result;
        try {
            result = new JSONObject(EntityUtils.toString(response.getEntity()));

            if (!ignoreErrors && result.get("status").equals("ERROR")) {
                // error handling from API calls
                JSONArray errors = result.getJSONArray("errors");

                if (errors.length() > 0) {
                    throw new SandboxException(errors.getJSONObject(0).getString("desc"), null);
                }
            }
        } catch (JSONException e) {
            throw new SandboxException("Error parsing REST API response", e);
        } catch (IOException e) {
            throw new SandboxException("Error parsing REST API response", e);
        }

        return result;
    }

    private static String[] split(final String s) {
        if (TextUtils.isBlank(s)) {
            return null;
        }
        return s.split(" *, *");
    }

    public String getUsername() {
        return username;
    }
}
