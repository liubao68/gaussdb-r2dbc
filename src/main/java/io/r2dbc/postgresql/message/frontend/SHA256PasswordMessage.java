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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.backend.AuthenticationSHA256Password;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.MD5Digest;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeBytes;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeInt;

/**
 * The SHA256PasswordMessage message.
 */
public final class SHA256PasswordMessage implements FrontendMessage {

    private final CharSequence password;

    private final String username;

    private final AuthenticationSHA256Password authentication;

    /**
     * Create a new message.
     *
     * @param password the password (encrypted, if requested)
     * @throws IllegalArgumentException if {@code password} is {@code null}
     */
    public SHA256PasswordMessage(String username, CharSequence password, AuthenticationSHA256Password authentication) {
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.authentication = Assert.requireNonNull(authentication, "authentication must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();
            byte[] result;
            if (AuthenticationSHA256Password.SHA256_PASSWORD == authentication.getPasswordStoredMethod() ||
                AuthenticationSHA256Password.PLAIN_PASSWORD == authentication.getPasswordStoredMethod()) {
                String randomCode = new String(authentication.getRandomCode().array(), StandardCharsets.UTF_8);
                String token = new String(authentication.getToken().array(), StandardCharsets.UTF_8);
                int iteration = authentication.getIteration().getInt();
                result = MD5Digest.RFC5802Algorithm(String.valueOf(this.password), randomCode, token, iteration);
            } else {
                byte[] salt = authentication.getMd5Salt().array();
                result = MD5Digest.SHA256_MD5encode(username.getBytes(StandardCharsets.UTF_8),
                    String.valueOf(this.password).getBytes(StandardCharsets.UTF_8), salt);
            }
            writeByte(out, 'p');
            writeInt(out, 4 + result.length + 1);
            writeBytes(out, ByteBuffer.wrap(result));
            return writeByte(out, 0);
        });
    }
}
