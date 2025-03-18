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

package io.r2dbc.gaussdb.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.gaussdb.message.backend.AuthenticationMD5SHA256Password;
import io.r2dbc.gaussdb.util.Assert;
import io.r2dbc.gaussdb.util.MD5Digest;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.r2dbc.gaussdb.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.gaussdb.message.frontend.FrontendMessageUtils.writeBytes;
import static io.r2dbc.gaussdb.message.frontend.FrontendMessageUtils.writeInt;

/**
 * The SHA256PasswordMessage message.
 */
public final class MD5SHA256PasswordMessage implements FrontendMessage {

    private final CharSequence password;

    private final AuthenticationMD5SHA256Password authentication;

    /**
     * Create a new message.
     *
     * @throws IllegalArgumentException if {@code password} is {@code null}
     */
    public MD5SHA256PasswordMessage(CharSequence password, AuthenticationMD5SHA256Password authentication) {
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.authentication = Assert.requireNonNull(authentication, "authentication must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();
            String randomCode = new String(authentication.getRandomCode().array(), StandardCharsets.UTF_8);
            byte[] result = MD5Digest.MD5_SHA256encode(String.valueOf(password), randomCode, authentication.getMd5Salt().array());
            writeByte(out, 'p');
            writeInt(out, 4 + result.length + 1);
            writeBytes(out, ByteBuffer.wrap(result));
            return writeByte(out, 0);
        });
    }

}
