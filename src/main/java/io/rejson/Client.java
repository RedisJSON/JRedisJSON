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

        DEL("JSON.DEL"),
        GET("JSON.GET"),
        SET("JSON.SET"),
        TYPE("JSON.TYPE");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    private void assertReplyNotError(final String str) throws Exception {
        if (str.startsWith("-ERR"))
            throw new Exception(str.substring(5));
    }

    private void assertReplyOK(final String str) throws Exception {
        if (!str.equals("OK"))
            throw new Exception(str);
    }

    /**
     * Create a new client
     * @param host the Redis host
     * @param port the Redis port
     * @param timeout the timeout
     * @param poolSize the pool's size
     */
    public Client(final String host, final int port, final int timeout, final int poolSize) {
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

    public Client(final String host, final int port) {
        this(host, port, 500, 100);
    }

    /**
     * Deletes a path
     * @param key the key name
     * @param path a path in the object
     * @return the number of paths deleted (0 or 1)
     */
    public Long del(final String key, final Path path) throws Exception {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(3);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        Long rep = conn.getClient()
                .sendCommand(Command.DEL, args.toArray(new byte[args.size()][]))
                .getIntegerReply();
        conn.close();

        return rep;
    }

    /**
     * Gets an object
     * @param key the key name
     * @param paths a path in the object
     * @return the requested object
     */
    public Object get(final String key, final Path... paths) throws Exception {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        for (Path p :paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }

        String rep = conn.getClient()
                .sendCommand(Command.GET, args.toArray(new byte[args.size()][]))
                .getBulkReply();
        conn.close();

        assertReplyNotError(rep);
        return gson.fromJson(rep, Object.class);
    }

    // TODO: add support for JSON.MGET

    /**
     * Sets an object
     * @param key the key name
     * @param path a path in the object
     * @param object the Java object to store
     */
    public void set(final String key, final Path path, final Object object) throws Exception {
        // TODO: support NX|XX flags
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(3);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));

        String status = conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]))
                .getStatusCodeReply();
        conn.close();

        assertReplyOK(status);
    }

    /**
     * Gets the class of an object
     * @param key the key name
     * @param path a path in the object
     * @return the Java class of the requested object
     */
    public Class<? extends Object> type(final String key, final Path path) throws Exception {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        String rep = conn.getClient()
                .sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]))
                .getBulkReply();
        conn.close();

        assertReplyNotError(rep);

        switch (rep) {
            case "null":
                return null;
            case "boolean":
                return boolean.class;
            case "integer":
                return int.class;
            case "number":
                return float.class;
            case "string":
                return String.class;
            case "object":
                return Object.class;
            case "array":
                return List.class;
            default:
                throw new Exception(rep);
        }
    }

}
