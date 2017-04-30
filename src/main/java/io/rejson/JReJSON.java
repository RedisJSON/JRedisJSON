package io.rejson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.gson.Gson;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.commands.*;
import redis.clients.util.SafeEncoder;

import java.util.*;

/**
 * JReJSON is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class JReJSON extends Jedis implements JedisCommands, MultiKeyCommands, AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands, ModuleCommands,IJReJSON {

    private JedisPool pool;
    private Gson gson;

    Jedis _conn() {
        return pool.getResource();
    }

    @Override
    public Object JSONGet(String key) {


        return null;
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

    /**
     * Existential modifier for the set command, by default we don't care
     */
    public enum ExistenceModifier implements ProtocolCommand {
        DEFAULT(""),
        NOT_EXISTS("NX"),
        MUST_EXIST("XX");
        private final byte[] raw;

        ExistenceModifier(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     *  Helper to check for errors and throw them as an exception
     * @param str the reply string to "analyze"
     * @throws Exception
     */
    private void assertReplyNotError(final String str) {
        if (str.startsWith("-ERR"))
            throw new RuntimeException(str.substring(5));
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private void assertReplyOK(final String str) {
        if (!str.equals("OK"))
            throw new RuntimeException(str);
    }

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length) {
            // default to root
            return Path.RootPath();
        } else if (1 == path.length){
            // take 1
            return path[0];
        } else {
            // throw out the baby with the water
            throw new RuntimeException("Only a single optional path is allowed");
        }
    }

    /**
     * Create a new client
     * @param host the Redis host
     * @param port the Redis port
     * @param timeout the timeout
     * @param poolSize the pool's size
     */
    public JReJSON(String host, int port, int timeout, int poolSize) {
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

    /**
     * Create a new client with default timeout and poolSize
     * @param host the Redis host
     * @param port the Redis port
     */
    public JReJSON(String host, int port) {
        this(host, port, 500, 100);
    }

    /**
     * Deletes a path
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     */
    public Long del(String key, Path... path) {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        Long rep = conn.getClient()
                    .sendCommand(Command.DEL, args.toArray(new byte[args.size()][]))
                    .getIntegerReply();
        conn.close();

        return rep;
    }

    /**
     * Gets an object
     * @param key the key name
     * @param paths optional one ore more paths in the object, defaults to root
     * @return the requested object
     */
    public Object get(String key, Path... paths) {
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
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path optional single path in the object, defaults to root
     */
    public void set(String key, Object object, ExistenceModifier flag, Path... path) {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        String status = conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]))
                .getStatusCodeReply();
        conn.close();

        assertReplyOK(status);
    }

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path optional single path in the object, defaults to root
     */
    public void set(String key, Object object, Path... path) {
        this.set(key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     */
    public Class<? extends Object> type(String key, Path... path) {
        Jedis conn = _conn();
        ArrayList<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

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
                throw new java.lang.RuntimeException(rep);
        }
    }
}
