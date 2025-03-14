/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.gaussdb.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.gaussdb.util.Assert;
import io.r2dbc.gaussdb.util.ByteBufferUtils;

import java.nio.ByteBuffer;

/**
 * The AuthenticationSHA256Password message.
 */
public final class AuthenticationSHA256Password implements AuthenticationMessage {
    public static final int PLAIN_PASSWORD = 0;

    public static final int MD5_PASSWORD = 1;

    public static final int SHA256_PASSWORD = 2;

    private final int passwordStoredMethod;

    private ByteBuffer randomCode;

    private ByteBuffer token;

    private ByteBuffer iteration;

    private ByteBuffer md5Salt;

    /**
     * Create a new message.
     *
     * @throws IllegalArgumentException if {@code authenticationMechanisms} is {@code null}
     */
    public AuthenticationSHA256Password(int passwordStoredMethod, ByteBuffer randomCode, ByteBuffer token, ByteBuffer iteration) {
        this.passwordStoredMethod = passwordStoredMethod;
        this.randomCode = randomCode;
        this.token = token;
        this.iteration = iteration;
    }

    public AuthenticationSHA256Password(int passwordStoredMethod, ByteBuffer md5Salt) {
        this.passwordStoredMethod = passwordStoredMethod;
        this.md5Salt = md5Salt;
    }

    public ByteBuffer getRandomCode() {
        return this.randomCode;
    }

    public ByteBuffer getToken() {
        return this.token;
    }

    public ByteBuffer getIteration() {
        return iteration;
    }

    public ByteBuffer getMd5Salt() {
        return this.md5Salt;
    }

    public int getPasswordStoredMethod() {
        return this.passwordStoredMethod;
    }

    static AuthenticationSHA256Password decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int passwordStoredMethod = in.readInt();
        if (passwordStoredMethod == PLAIN_PASSWORD || passwordStoredMethod == SHA256_PASSWORD) {
            return new AuthenticationSHA256Password(passwordStoredMethod,
                ByteBufferUtils.toByteBuffer(in.readSlice(64)),
                ByteBufferUtils.toByteBuffer(in.readSlice(8)),
                ByteBufferUtils.toByteBuffer(in.readSlice(4)));
        } else if(passwordStoredMethod == MD5_PASSWORD) {
            return new AuthenticationSHA256Password(passwordStoredMethod, ByteBufferUtils.toByteBuffer(in.readSlice(4)));
        }
        throw new IllegalArgumentException(String.format("%s is not a supported password stored method.", passwordStoredMethod));
    }

}
