package com.mapr.rest;

import com.mapr.db.sandbox.SandboxException;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class MapRRestClient {
    private final String username;
    private final String password;
    private final String baseUrl;


    final HttpClient client;

    public MapRRestClient(String hostname, String username, String password) throws SandboxException {
        this.baseUrl = String.format("https://%s/rest", hostname);
        this.username = username;
        this.password = password;

        // TODO convert to singleton?
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
        provider.setCredentials(AuthScope.ANY, credentials);

        try {
            // TODO specify the supported ciphers in config
            client = HttpClientBuilder.create()
                    .setSSLSocketFactory(new SSLConnectionSocketFactory(
                            SSLContexts.custom()
                                    .loadTrustMaterial(new TrustSelfSignedStrategy()).build(),
                            split(System.getProperty("https.protocols", "TLSv1.2")),
                            split("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_RSA_WITH_RC4_128_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA,SSL_RSA_WITH_RC4_128_SHA"),
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
}
