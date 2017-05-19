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

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class UsageExampleTest {

    private String host = "localhost";
    private int port = 6379;
    Jedis jedis = new Jedis(host,port);

    @Before
    public void initialize() {
        jedis.flushDB();
    }

    @Test
    public void exampleShouldWork() throws Exception {

        // Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
        JReJSON.set(jedis,"foo", "bar");
        String s0 = (String) JReJSON.get(jedis,"foo");

        // Omitting the path (usually) defaults to the root path, so the call above to `get()` and the following ones
        // are basically interchangeable
        String s1 = (String) JReJSON.get(jedis,"foo", new Path("."));
        String s2 = (String) JReJSON.get(jedis, "foo", Path.RootPath());

        // Any Gson-able object can be set and updated
        JReJSON.set(jedis,"obj", new Object());					         // just an empty object
        JReJSON.set(jedis,"obj", null, new Path(".zilch"));
        Path p = new Path(".whatevs");
        JReJSON.set(jedis,"obj", true, p);
        JReJSON.set(jedis,"obj", 42, p);
        JReJSON.del(jedis,"obj", p);                                    // back to almost nothing
    }
}
