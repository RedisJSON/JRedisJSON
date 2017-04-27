package io.rejson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.gson.Gson;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.util.SafeEncoder;

import java.util.*;

/**
 * Client is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class Client {

    private JedisPool pool;
    private Gson gson;

    Jedis _conn() {
        return pool.getResource();
    }

    private enum Command implements ProtocolCommand {

        SET("JSON.SET"),
        GET("JSON.GET");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    private void assertReplyNotError(String str) throws Exception {
        if (str.startsWith("-ERR"))
            throw new Exception(str.substring(5));
    }

    private void assertReplyOK(String str) throws Exception {
        if (!str.equals("OK"))
            throw new Exception(str);
    }

    /**
     * Create a new client
     * @param host the redis host
     * @param port the redis port
     */
    public Client(String host, int port, int timeout, int poolSize) {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(poolSize);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);
        conf.setFairness(true);

        pool = new JedisPool(conf, host, port, timeout);
        gson = new Gson();

    }

    public Client(String host, int port) {
        this(host, port, 500, 100);
    }

    /**
     * Sets an object
     * @param key the key name
     * @param path a path in the object
     * @param object the Java object to store
     */
    public void set(String key, String path, Object object) throws Exception {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(3);

        args.add(key.getBytes());
        args.add(path.getBytes());
        args.add(gson.toJson(object).getBytes());

        String status = conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]))
                .getStatusCodeReply();
        conn.close();

        assertReplyOK(status);
    }

    /**
     * Gets an object
     * @param key the key name
     * @param path a path in the object
     * @return the requested object
     */
    public Object get(String key, String path) throws Exception {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(2);

        args.add(key.getBytes());
        args.add(path.getBytes());

        String rep = conn.getClient()
                .sendCommand(Command.GET, args.toArray(new byte[args.size()][]))
                .getBulkReply();
        conn.close();

        assertReplyNotError(rep);
        return gson.fromJson(rep, Object.class);
    }

}
