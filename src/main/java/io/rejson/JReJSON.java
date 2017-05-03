package io.rejson;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * JReJSON is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class JReJSON {

    private static Gson gson = new Gson();

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
     * @throws RuntimeException
     */
    private static void assertReplyNotError(final String str) {
        if (str.startsWith("-ERR"))
            throw new RuntimeException(str.substring(5));
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final String str) {
        if (!str.equals("OK"))
            throw new RuntimeException(str);
    }

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length)
            // default to root
            return Path.RootPath();
         else if (1 == path.length)
            // take 1
            return path[0];
         else
            // throw out the baby with the water
            throw new RuntimeException("Only a single optional path is allowed");

    }



    /**
     * Deletes a path
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     */
    public static Long del(Jedis conn, String key, Path... path) {

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
    public static Object get(Jedis conn,String key, Path... paths) {

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
    public static void set(Jedis conn ,String key, Object object, ExistenceModifier flag, Path... path) {

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
    public static void set(Jedis conn,String key, Object object, Path... path) {
        set(conn,key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     */
    public static Class<? extends Object> type(Jedis conn,String key, Path... path) {

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
