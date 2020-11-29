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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class UsageExampleTest {

    private String host = "localhost";
    private int port = 6379;
    Jedis jedis = new Jedis(host,port);
    JReJSON reJSON = new JReJSON(host,port);

    @Before
    public void initialize() {
        jedis.flushDB();
    }

    @Test
    public void exampleShouldWork() throws Exception {

        // Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
        reJSON.set("foo", "bar");
        String s0 = reJSON.get("foo");
        assertEquals("bar", s0);

        // Omitting the path (usually) defaults to the root path, so the call above to `get()` and the following ones
        // are basically interchangeable
        String s1 = reJSON.get("foo", String.class, new Path("."));
        String s2 = reJSON.get( "foo", String.class, Path.ROOT_PATH);
        
        assertEquals("bar", s1);
        assertEquals("bar", s2);

        // Any Gson-able object can be set and updated
        reJSON.set("obj", new Object()); // just an empty object
        reJSON.set("obj", null, new Path(".zilch"));
        Path p = new Path(".whatevs");
        reJSON.set("obj", true, p);
        reJSON.set("obj", 42, p);
        
        Object obj = reJSON.get("obj");
        assertEquals("{zilch=null, whatevs=42.0}", obj.toString());
        
        reJSON.del("obj", p); // back to almost nothing
    }
}
