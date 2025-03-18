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
public final class AuthenticationMD5SHA256Password implements AuthenticationMessage {

    private ByteBuffer randomCode;

    private ByteBuffer md5Salt;

    /**
     * Create a new message.
     *
     * @throws IllegalArgumentException if {@code authenticationMechanisms} is {@code null}
     */
    public AuthenticationMD5SHA256Password(ByteBuffer randomCode, ByteBuffer md5Salt) {
        this.randomCode = randomCode;
        this.md5Salt = md5Salt;
    }

    public ByteBuffer getRandomCode() {
        return this.randomCode;
    }

    public ByteBuffer getMd5Salt() {
        return this.md5Salt;
    }

    static AuthenticationMD5SHA256Password decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");
        return new AuthenticationMD5SHA256Password(ByteBufferUtils.toByteBuffer(in.readSlice(64)),
            ByteBufferUtils.toByteBuffer(in.readSlice(4)));
    }

}
