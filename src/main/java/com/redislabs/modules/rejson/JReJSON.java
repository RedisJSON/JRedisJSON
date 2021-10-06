/*
 * BSD 2-Clause License
 *
 * Copyright (c) 2017, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.redislabs.modules.rejson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

/**
 * JReJSON is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class JReJSON {

    private static final Gson gson = new Gson();

    private enum Command implements ProtocolCommand {
        DEL("JSON.DEL"),
        GET("JSON.GET"),
        MGET("JSON.MGET"),
        SET("JSON.SET"),
        TYPE("JSON.TYPE"),
        STRAPPEND("JSON.STRAPPEND"),
        STRLEN("JSON.STRLEN"),
        ARRAPPEND("JSON.ARRAPPEND"),
        ARRINDEX("JSON.ARRINDEX"),
        ARRINSERT("JSON.ARRINSERT"),
        ARRLEN("JSON.ARRLEN"),
        ARRPOP("JSON.ARRPOP"),
        ARRTRIM("JSON.ARRTRIM"),
        CLEAR("JSON.CLEAR"),
        TOGGLE("JSON.TOGGLE");

        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        @Override
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

        @Override
        public byte[] getRaw() {
            return raw;
        }
    }

	private Pool<Jedis> client;

    /**
     * Creates a client to the local machine
     */
    public JReJSON() {
        this("localhost", 6379);
    }

    /**
     * Creates a client to the specific host/post
     *
     * @param host Redis host
     * @param port Redis port
     */
    public JReJSON(String host, int port) {
        this(new JedisPool(host, port));
    }

    /**
     * Creates a client using provided Jedis pool
     *
     * @param jedis bring your own Jedis pool
     */
    public JReJSON(Pool<Jedis> jedis) {
        this.client = jedis;
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final String str) {
        if (str == null) {
            throw new NullPointerException("Null response received.");
        }
    }

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length) {   // default to root
            return Path.ROOT_PATH;
        }
        if (1 == path.length) {  // take 1
            return path[0];
        }

        // throw out the baby with the water
        throw new RuntimeException("Only a single optional path is allowed");
    }

    /**
     * Deletes the root path
     * @param key the key name
     * @return the number of paths deleted (0 or 1)
     */
    public Long del(String key) {
    	return del(key, Path.ROOT_PATH);
    }

    /**
     * Deletes a path
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return path deleted
     */
    public Long del(String key, Path path) {
    	byte[][] args = new byte[2][];
    	args[0] = SafeEncoder.encode(key);
    	args[1] = SafeEncoder.encode(path.toString());

    	try (Jedis conn = getConnection()) {
    		conn.getClient().sendCommand(Command.DEL, args);
    		return conn.getClient().getIntegerReply();
    	}
    }

    public long clear(String key, Path path) {
    	  try (Jedis jedis = getConnection()) {
    		    jedis.getClient().sendCommand(Command.CLEAR, key, path.toString());
    		    return jedis.getClient().getIntegerReply();
    	  }
    }

    /**
     * Gets an object at the root path
     * @param <T> type of data represented at {@code key}
     * @param key the key name
     * @return the requested object
     */
    public <T> T get(String key) {
    	return get(key, Path.ROOT_PATH);
    }

    /**
     * Gets an object
     * @param <T> type of data represented at {@code key}
     * @param key the key name
     * @param paths optional one ore more paths in the object
     * @return the requested object
     * @deprecated use {@link #get(String, Class, Path...)} instead
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public <T> T get(String key, Path... paths) {
      return (T)this.get(key, Object.class, paths);
    }

    /**
     * Gets an object
     * @param <T> type of data represented at {@code key}
     * @param key the key name
     * @param clazz Class representing {@code T}
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    public <T> T get(String key, Class<T> clazz, Path... paths) {
        byte[][] args = new byte[1 + paths.length][];
        int i=0;
        args[i] = SafeEncoder.encode(key);
        for (Path p :paths) {
        	args[++i] = SafeEncoder.encode(p.toString());
        }

        String rep;
    	try (Jedis conn = getConnection()) {
    		conn.getClient().sendCommand(Command.GET, args);
        	rep = conn.getClient().getBulkReply();
    	}

    	return gson.fromJson(rep, clazz);
    }

    /**
     * Returns the documents from multiple keys. Non-existing keys are reported as
     * null.
     *
     * @param <T>   target class to serialize results
     * @param clazz target class to serialize results
     * @param keys  keys for the JSON documents
     * @return a List of documents rooted at path
     */
    public <T> List<T> mget(Class<T> clazz, String... keys) {
        return mget(Path.ROOT_PATH, clazz, keys);
    }

    /**
     * Returns the values at path from multiple keys. Non-existing keys and
     * non-existing paths are reported as null.
     *
     * @param path  common path across all documents to root the results on
     * @param <T>   target class to serialize results
     * @param clazz target class to serialize results
     * @param keys  keys for the JSON documents
     * @return a List of documents rooted at path
     */
    public <T> List<T> mget(Path path, Class<T> clazz, String... keys) {
        String[] args = Stream //
                .of(keys, new String[] { path.toString() }) //
                .flatMap(Stream::of) //
                .toArray(String[]::new);

        List<String> rep;
        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.MGET, args);
            rep = conn.getClient().getMultiBulkReply();
        }

        return rep.stream() //
                .map(r -> gson.fromJson(r, clazz)) //
                .collect(Collectors.toList());
    }

    /**
     * Sets an object at the root path
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     */
    public void set(String key, Object object, ExistenceModifier flag) {
    	set(key, object, flag, Path.ROOT_PATH);
    }

    /**
     * Sets an object in the root path
     * @param key the key name
     * @param object the Java object to store
     */
    public void set(String key, Object object) {
        set(key, object, ExistenceModifier.DEFAULT, Path.ROOT_PATH);
    }

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path in the object
     */
    public void set(String key, Object object, Path path) {
        set(key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Sets an object
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path in the object
     */
    public void set(String key, Object object, ExistenceModifier flag, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        String status;
    	try (Jedis conn = getConnection()) {
	        conn.getClient()
	                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
	        status = conn.getClient().getStatusCodeReply();
    	}
        assertReplyOK(status);
    }

    public void toggle(String key, Path path) {
        try (Jedis jedis = getConnection()) {
            jedis.getClient().sendCommand(Command.TOGGLE, key, path.toString());
            assertReplyOK(jedis.getClient().getStatusCodeReply());
        }
    }

    /**
     * Gets the class of an object at the root path
     * @param key the key name
     * @return the Java class of the requested object
     */
    public Class<?> type(String key) {
    	return type(key, Path.ROOT_PATH);
    }

    /**
     * Gets the class of an object
     * @param key the key name
     * @param path a path in the object
     * @return the Java class of the requested object
     */
    public Class<?> type(String key, Path path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        String rep;
    	try (Jedis conn = getConnection()) {
    		conn.getClient()
                .sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
        	rep = conn.getClient().getBulkReply();
    	}

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
                throw new RuntimeException(rep);
        }
    }

    /**
     * Deletes a path
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     * @deprecated use {@link #del(String, Path)} instead
     */
    @Deprecated
    public static Long del(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient()
                    .sendCommand(Command.DEL, args.toArray(new byte[args.size()][]));
        Long rep = conn.getClient().getIntegerReply();
        conn.close();

        return rep;
    }

    /**
     * Gets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param paths optional one ore more paths in the object, defaults to root
     * @return the requested object
     * @deprecated use {@link #get(String, Path...)} instead
     */
    @Deprecated
    public static Object get(Jedis conn, String key, Path... paths) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        for (Path p :paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }

        conn.getClient()
                .sendCommand(Command.GET, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();
        conn.close();

        return gson.fromJson(rep, Object.class);
    }

    /**
     * Sets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path optional single path in the object, defaults to root
     * @deprecated use {@link #set(String, Object, ExistenceModifier, Path)} instead
     */
    @Deprecated
    public static void set(Jedis conn, String key, Object object, ExistenceModifier flag, Path... path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        String status = conn.getClient().getStatusCodeReply();
        conn.close();

        assertReplyOK(status);
    }

    /**
     * Sets an object without caring about target path existing
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param path optional single path in the object, defaults to root
     * @deprecated use {@link #set(String, Object, ExistenceModifier, Path)} instead
     */
    @Deprecated
    public static void set(Jedis conn, String key, Object object, Path... path) {
        set(conn,key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     * @deprecated use {@link #type(String, Path)} instead
     */
    @Deprecated
    public static Class<?> type(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient()
                .sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();
        conn.close();

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
                throw new RuntimeException(rep);
        }
    }

    private Jedis getConnection() {
        return this.client.getResource();
    }

    /**
     * Append the value(s) the string at path.
     *
     * Returns the string's new size.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @param objects objects One or more elements to be added to the array
     * @return the size of the modified string
     */
    public Long strAppend(String key, Path path, Object... objects) {
        List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        args.addAll(Arrays.stream(objects) //
                .map(object -> SafeEncoder.encode(gson.toJson(object))) //
                .collect(Collectors.toList()));

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.STRAPPEND, args.toArray(new byte[args.size()][]));
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Report the length of the JSON String at path in key.
     * Path defaults to root if not provided.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @return the size of string at path. If the key or path do not exist, null is returned.
     */
    public Long strLen(String key, Path path) {
        byte[][] args = new byte[2][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.STRLEN, args);
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Appends elements into the array at path.
     *
     * Returns the array's new size.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @param objects one or more elements to be added to the array
     * @return the size of the modified array
     */
    public Long arrAppend(String key, Path path, Object... objects) {
        List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        args.addAll(Arrays.stream(objects) //
                .map(object -> SafeEncoder.encode(gson.toJson(object))) //
                .collect(Collectors.toList()));

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRAPPEND, args.toArray(new byte[args.size()][]));
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Finds the index of the first occurrence of a scalar JSON value in the array
     * at the given path.
     *
     * If the item is not found, it returns -1. If called on a key path that is not
     * an array, it throws an error.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @param scalar the JSON scalar to search for
     * @return the index of the element if found, -1 if not found
     */
    public Long arrIndex(String key, Path path, Object scalar) {
        byte[][] args = new byte[3][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());
        args[2] = SafeEncoder.encode(gson.toJson(scalar));

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRINDEX, args);
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Insert element(s) into the array at path before the index (shifts to the right).
     * The index must be in the array's range. Inserting at index 0 prepends to the array.
     * Negative index values are interpreted as starting from the end.
     *
     * Returns the array's new size.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @param index position in the array to insert the value(s)
     * @param objects one or more elements to be added to the array
     * @return the size of the modified array
     */
    public Long arrInsert(String key, Path path, Long index, Object... objects) {
        List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(Protocol.toByteArray(index));

        args.addAll(Arrays.stream(objects) //
                .map(object -> SafeEncoder.encode(gson.toJson(object))) //
                .collect(Collectors.toList()));

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRINSERT, args.toArray(new byte[args.size()][]));
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Get the number of elements for an array field (for a given path)
     *
     * If called on a key path that is not an array, it will throw an error.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @return the size of array at path
     */
    public Long arrLen(String key, Path path) {
        byte[][] args = new byte[2][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRLEN, args);
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Remove and return element from the index in the array.
     *
     * path defaults to root if not provided. index is the position in the array to start
     * popping from (defaults to -1, meaning the last element). Out of range indices are
     * rounded to their respective array ends. Popping an empty array yields null.
     *
     * @param <T> type of data represented at {@code key}
     * @param key the key of the value
     * @param clazz target class to serialize results
     * @param path the path of the value
     * @param index the position in the array to start popping from
     * @return the popped JSON value.
     */
    public <T> T arrPop(String key, Class<T> clazz, Path path, Long index) {
        List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path != null ? path.toString() :Path.ROOT_PATH.toString()));
        args.add(Protocol.toByteArray(index != null ? index : -1));

        String rep;
        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRPOP, args.toArray(new byte[args.size()][]));
            rep = conn.getClient().getBulkReply();
        }

        return gson.fromJson(rep, clazz);
    }

    /**
     * Remove and return element from the index in the array.
     *
     * path defaults to root if not provided. index is the position in the array to start
     * popping from (defaults to -1, meaning the last element). Out of range indices are
     * rounded to their respective array ends. Popping an empty array yields null.
     *
     * @param <T> type of data represented at {@code key}
     * @param key the key of the value
     * @param clazz target class to serialize results
     * @param path the path of the value
     * @return the popped JSON value.
     */
    public <T> T arrPop(String key, Class<T> clazz, Path path) {
        return arrPop(key, clazz, path, null);
    }

    /**
     * Remove and return element from the index in the array.
     *
     * path defaults to root if not provided. index is the position in the array to start
     * popping from (defaults to -1, meaning the last element). Out of range indices are
     * rounded to their respective array ends. Popping an empty array yields null.
     *
     * @param <T> type of data represented at {@code key}
     * @param key the key of the value
     * @param clazz target class to serialize results
     * @return the popped JSON value.
     */
    public <T> T arrPop(String key, Class<T> clazz) {
        return arrPop(key, clazz, null, null);
    }

    /**
     * Trim an array so that it contains only the specified inclusive range of elements.
     *
     * This command is extremely forgiving and using it with out of range indexes will not produce
     * an error. If start is larger than the array's size or start &gt; stop, the result will be an
     * empty array. If start is &lt; 0 then it will be treated as 0. If stop is larger than the end
     * of the array, it will be treated like the last element in it.
     *
     * @param key the key of the value
     * @param path the path of the value
     * @param start the start of the range
     * @param stop the end of the range
     * @return the array's new size
     */
    public Long arrTrim(String key, Path path, Long start, Long stop) {
        List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(Protocol.toByteArray(start));
        args.add(Protocol.toByteArray(stop));

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.ARRTRIM, args.toArray(new byte[args.size()][]));
            return conn.getClient().getIntegerReply();
        }
    }
}
