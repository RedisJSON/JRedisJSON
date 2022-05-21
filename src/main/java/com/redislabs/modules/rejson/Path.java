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

import java.util.Objects;

/**
 * Path is a ReJSON path, representing a valid path into an object
 */
public class Path {
    
    public static final Path ROOT_PATH = new Path(".");
    
    private final String strPath;

    public Path(final String strPath) {
        this.strPath = strPath;
    }

    /**
     * Makes a root path
     * @return the root path
     * @deprecated use {@link #ROOT_PATH} instead
     */
    @Deprecated
    public static Path RootPath() {
        return new Path(".");
    }

    @Override
    public String toString() {
        return strPath;
    }
    
    public static Path of(final String strPath) {
        return new Path(strPath);
    }
    
    public static Path ofJsonPointer(final String strPath) {
        return new Path(parse(strPath));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof Path)) return false;
        if (obj == this) return true;
        return this.toString().equals(((Path) obj).toString());
    }
    
    @Override
    public int hashCode() {
        return strPath.hashCode();
    }
    
    private static String parse(String path) {
        Objects.requireNonNull(path, "Json Pointer Path cannot be null.");
        
        if (path.isEmpty()) {
            // "" means all document 
            return ROOT_PATH.toString();
        }
        if (path.charAt(0) != '/') {
            throw new IllegalArgumentException("Json Pointer Path must start with '/'.");
        }
        
        final char[] ary = path.toCharArray();
        StringBuilder result = new StringBuilder();
        StringBuilder builder = new StringBuilder();
        boolean number = true;
        char prev = '/';
        for (int i = 1; i < ary.length; i++) {
            char c = ary[i];
            switch (c) {
            case '~':
                if (prev == '~') {
                    number = false;
                    builder.append('~');
                }
                break;
            case '/':
                if (prev == '~') {
                    number = false;
                    builder.append('~');
                }
                if (builder.length() > 0 && number) {
                    result.append(".[").append(builder).append("]");
                } else {
                    result.append(".[\"").append(builder).append("\"]");
                }
                number = true;
                builder.setLength(0);
                break;
            case '0':
                if (prev == '~') {
                    number = false;
                    builder.append("~");
                } else {
                    builder.append(c);
                }
                break;
            case '1':
                if (prev == '~') {
                    number = false;
                    builder.append("/");
                } else {
                    builder.append(c);
                }
                break;
            default:
                if (prev == '~') {
                    number = false;
                    builder.append('~');
                }
                if (c < '0' || c > '9') {
                    number = false;
                }
                builder.append(c);
                break;
            }
            prev = c;
        }
        if (prev == '~') {
            number = false;
            builder.append("~");
        }
        if (builder.length() > 0) {
            if (number) {
                result.append(".[").append(builder).append("]");
            } else {
                result.append(".[\"").append(builder).append("\"]");
            }
        } else if (prev == '/') {
            result.append(".[\"").append(builder).append("\"]");
        }
        return result.toString();
    }
}
