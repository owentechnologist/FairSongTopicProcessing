package com.redislabs.sa.ot.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.providers.PooledConnectionProvider;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.*;
import java.util.ArrayList;
import java.util.Arrays;

public class JedisPooledHelper {

    private static JedisPooled jedisPooled=null;

    public static JedisPooled getJedisPooledFromArgs(String[]args){
        long startTime = System.currentTimeMillis();
        jedisPooled = initRedisConnection(args);
        System.out.println("\n\t\tTIME in MILLISECONDS TAKEN TO CREATE REDIS CONNECTION - "+(System.currentTimeMillis()-startTime));
        return jedisPooled;
    }

    /**
     * might return null!
     * @return
     */
    public static JedisPooled getJedisPooled(){
        return jedisPooled;
    }

    //connection establishment
    private static JedisPooled initRedisConnection(String[] args){
        String username = "default"; // we assume default user for initial tests
        String host = "localhost";
        int port = 6379;
        String password = "";
        JedisConnectionHelperSettings settings = new JedisConnectionHelperSettings();

        ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
        if (argList.contains("--host")) {
            int hostIndex = argList.indexOf("--host");
            host = argList.get(hostIndex + 1);
            settings.setRedisHost(host);
        }
        if (argList.contains("--port")) {
            int portIndex = argList.indexOf("--port");
            port = Integer.parseInt(argList.get(portIndex + 1));
            settings.setRedisPort(port);
        }
        if (argList.contains("--user")) {
            int userNameIndex = argList.indexOf("--user");
            username = argList.get(userNameIndex + 1);
            settings.setUserName(username);
        }
        if (argList.contains("--password")) {
            int passwordIndex = argList.indexOf("--password");
            password = argList.get(passwordIndex + 1);
            settings.setPassword(password);
            settings.setUsePassword(true);
        }
        if (argList.contains("--usessl")) {
            int argIndex = argList.indexOf("--usessl");
            boolean useSSL = Boolean.parseBoolean(argList.get(argIndex + 1));
            System.out.println("loading custom --usessl == " + useSSL);
            settings.setUseSSL(useSSL);
        }
        if (argList.contains("--cacertpath")) {
            int argIndex = argList.indexOf("--cacertpath");
            String caCertPath = argList.get(argIndex + 1);
            System.out.println("loading custom --cacertpath == " + caCertPath);
            settings.setCaCertPath(caCertPath);
        }
        if (argList.contains("--cacertpassword")) {
            int argIndex = argList.indexOf("--cacertpassword");
            String caCertPassword = argList.get(argIndex + 1);
            System.out.println("loading custom --cacertpassword == " + caCertPassword);
            settings.setCaCertPassword(caCertPassword);
        }
        if (argList.contains("--usercertpath")) {
            int argIndex = argList.indexOf("--usercertpath");
            String userCertPath = argList.get(argIndex + 1);
            System.out.println("loading custom --usercertpath == " + userCertPath);
            settings.setUserCertPath(userCertPath);
        }
        if (argList.contains("--usercertpass")) {
            int argIndex = argList.indexOf("--usercertpass");
            String userCertPassword = argList.get(argIndex + 1);
            System.out.println("loading custom --usercertpass == " + userCertPassword);
            settings.setUserCertPassword(userCertPassword);
        }
        settings.setTestOnBorrow(true);
        settings.setConnectionTimeoutMillis(120000);
        settings.setNumberOfMinutesForWaitDuration(1);
        settings.setNumTestsPerEvictionRun(10);
        settings.setPoolMaxIdle(1); //this means less stale connections
        settings.setPoolMinIdle(0);
        settings.setRequestTimeoutMillis(12000);
        settings.setTestOnReturn(false); // if idle, they will be mostly removed anyway
        settings.setTestOnCreate(true);

        JedisConnectionHelper connectionHelper = null;
        try{
            // only use a single connection based on the hostname (not ipaddress) if possible
            connectionHelper = new JedisConnectionHelper(settings);
        }catch(Throwable t){
            t.printStackTrace();
            try{
                Thread.sleep(4000);
            }catch(InterruptedException ie){}
            // give it another go - in case the first attempt was just unlucky:
            // only use a single connection based on the hostname (not ipaddress) if possible
            connectionHelper = new JedisConnectionHelper(settings);
        }
        return connectionHelper.getPooledJedis();
    }
}


class JedisConnectionHelper {
    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    /**
     * Used when you want to send a batch of commands to the Redis Server
     * @return Pipeline
     */
    public Pipeline getPipeline(){
        return  new Pipeline(jedisPooled.getPool().getResource());
    }


    /**
     * Assuming use of Jedis 4.3.1:
     * https://github.com/redis/jedis/blob/82f286b4d1441cf15e32cc629c66b5c9caa0f286/src/main/java/redis/clients/jedis/Transaction.java#L22-L23
     * @return Transaction
     */
    public Transaction getTransaction(){
        return new Transaction(getPooledJedis().getPool().getResource());
    }

    /**
     * Obtain the default object used to perform Redis commands
     * @return JedisPooled
     */
    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    /**
     * Use this to build the URI expected in this classes' Constructor
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     */
    public static URI buildURI(String host, int port, String username, String password){
        URI uri = null;
        try {
            if (!("".equalsIgnoreCase(password))) {
                uri = new URI("redis://" + username + ":" + password + "@" + host + ":" + port);
            } else {
                uri = new URI("redis://" + host + ":" + port);
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        return uri;
    }

    private static SSLSocketFactory createSslSocketFactory(
            String caCertPath, String caCertPassword, String userCertPath, String userCertPassword)
            throws IOException, GeneralSecurityException {

        KeyStore keyStore = KeyStore.getInstance("pkcs12");
        keyStore.load(new FileInputStream(userCertPath), userCertPassword.toCharArray());

        KeyStore trustStore = KeyStore.getInstance("jks");
        trustStore.load(new FileInputStream(caCertPath), caCertPassword.toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        trustManagerFactory.init(trustStore);

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("PKIX");
        keyManagerFactory.init(keyStore, userCertPassword.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext.getSocketFactory();
    }

    public JedisConnectionHelper(JedisConnectionHelperSettings bs){
        System.out.println("Creating JedisConnectionHelper with "+bs);
        URI uri = buildURI(bs.getRedisHost(), bs.getRedisPort(), bs.getUserName(),bs.getPassword());
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        if(bs.isUsePassword()){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            password = password.split("@")[0];
            System.out.println("\n\nUsing user: "+user+" / password l!3*^rs@"+password);
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).socketTimeoutMillis(bs.getRequestTimeoutMillis()).build(); // timeout and client settings

        }
        else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).socketTimeoutMillis(bs.getRequestTimeoutMillis()).build(); // timeout and client settings
        }
        if(bs.isUseSSL()){ // manage client-side certificates to allow SSL handshake for connections
            SSLSocketFactory sslFactory = null;
            try{
                sslFactory = createSslSocketFactory(
                        bs.getCaCertPath(),
                        bs.getCaCertPassword(), // use the password you specified for keytool command
                        bs.getUserCertPath(),
                        bs.getUserCertPassword() // use the password you specified for openssl command
                );
            }catch(Throwable sslStuff){
                sslStuff.printStackTrace();
                System.exit(1);
            }
            clientConfig = DefaultJedisClientConfig.builder().user(bs.getUserName()).password(bs.getPassword())
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).socketTimeoutMillis(bs.getRequestTimeoutMillis())
                    .sslSocketFactory(sslFactory) // key/trust details
                    .ssl(true).build();
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(bs.getPoolMaxIdle());
        poolConfig.setMaxTotal(bs.getMaxConnections());
        poolConfig.setMinIdle(bs.getPoolMinIdle());
        poolConfig.setMaxWait(Duration.ofMinutes(bs.getNumberOfMinutesForWaitDuration()));
        poolConfig.setTestOnCreate(bs.isTestOnCreate());
        poolConfig.setTestOnBorrow(bs.isTestOnBorrow());
        poolConfig.setNumTestsPerEvictionRun(bs.getNumTestsPerEvictionRun());
        poolConfig.setBlockWhenExhausted(bs.isBlockWhenExhausted());
        poolConfig.setMinEvictableIdleTime(Duration.ofMillis(bs.getMinEvictableIdleTimeMilliseconds()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(bs.getTimeBetweenEvictionRunsMilliseconds()));

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}


class JedisConnectionHelperSettings {
    private String redisHost = "FIXME";
    private int redisPort = 6379;
    private String userName = "default";
    private String password = "";
    private int maxConnections = 10; // these are best shared
    private int connectionTimeoutMillis = 1000;
    private int requestTimeoutMillis = 200;
    private int poolMaxIdle = 5;
    private int poolMinIdle = 0;
    private int numberOfMinutesForWaitDuration = 1;
    private boolean testOnCreate = true;
    private boolean testOnBorrow = true;
    private boolean testOnReturn = true;
    private int numTestsPerEvictionRun = 3;
    private boolean useSSL = false;
    private boolean usePassword = false;
    private long minEvictableIdleTimeMilliseconds = 30000;
    private long timeBetweenEvictionRunsMilliseconds = 1000;
    private boolean blockWhenExhausted = true;
    private String trustStoreFilePath = "";
    private String trustStoreType = "";
    private String caCertPath = "./truststore.jks";
    private String caCertPassword = "FIXME";
    private String userCertPath = "./redis-user-keystore.p12";
    private String userCertPassword = "FIXME";

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public void setMaxConnectionsWithDerivedMaxMinIdleSideEffects(int maxConnections) {
        this.maxConnections = maxConnections;
        this.setPoolMaxIdle(Math.round(maxConnections/2));
        this.setPoolMinIdle(Math.round(maxConnections/10));
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public void setConnectionTimeoutMillis(int connectionTimeoutMillis) {
        this.connectionTimeoutMillis = connectionTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public int getPoolMaxIdle() {
        return poolMaxIdle;
    }

    public void setPoolMaxIdle(int poolMaxIdle) {
        this.poolMaxIdle = poolMaxIdle;
    }

    public int getPoolMinIdle() {
        return poolMinIdle;
    }

    public void setPoolMinIdle(int poolMinIdle) {
        this.poolMinIdle = poolMinIdle;
    }

    public int getNumberOfMinutesForWaitDuration() {
        return numberOfMinutesForWaitDuration;
    }

    public void setNumberOfMinutesForWaitDuration(int numberOfMinutesForWaitDuration) {
        this.numberOfMinutesForWaitDuration = numberOfMinutesForWaitDuration;
    }

    public boolean isTestOnCreate() {
        return testOnCreate;
    }

    public void setTestOnCreate(boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public boolean isUsePassword() {
        return usePassword;
    }

    public void setUsePassword(boolean usePassword) {
        this.usePassword = usePassword;
    }

    public long getMinEvictableIdleTimeMilliseconds() {
        return minEvictableIdleTimeMilliseconds;
    }

    public void setMinEvictableIdleTimeMilliseconds(long minEvictableIdleTimeMilliseconds) {
        this.minEvictableIdleTimeMilliseconds = minEvictableIdleTimeMilliseconds;
    }

    public long getTimeBetweenEvictionRunsMilliseconds() {
        return timeBetweenEvictionRunsMilliseconds;
    }

    public void setTimeBetweenEvictionRunsMilliseconds(long timeBetweenEvictionRunsMilliseconds) {
        this.timeBetweenEvictionRunsMilliseconds = timeBetweenEvictionRunsMilliseconds;
    }

    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    public String toString(){
        return "\nRedisUserName = "+getUserName()+"\nUsePassword = "+isUsePassword()+"\nUseSSL = "+isUseSSL()+ "\nRedisHost = "+getRedisHost()+
                "\nRedisPort = "+getRedisPort()+"\nMaxConnections = "+getMaxConnections()+
                "\nRequestTimeoutMilliseconds = "+getRequestTimeoutMillis()+"\nConnectionTimeOutMilliseconds = "+
                getConnectionTimeoutMillis();
    }

    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    public void setTrustStoreFilePath(String trustStoreFilePath) {
        this.trustStoreFilePath = trustStoreFilePath;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public String getCaCertPath() {
        return caCertPath;
    }

    public void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }

    public String getCaCertPassword() {
        return caCertPassword;
    }

    public void setCaCertPassword(String caCertPassword) {
        this.caCertPassword = caCertPassword;
    }

    public String getUserCertPath() {
        return userCertPath;
    }

    public void setUserCertPath(String userCertPath) {
        this.userCertPath = userCertPath;
    }

    public String getUserCertPassword() {
        return userCertPassword;
    }

    public void setUserCertPassword(String userCertPassword) {
        this.userCertPassword = userCertPassword;
    }
}

