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

import io.r2dbc.gaussdb.client.TestStartupParameterProvider;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.gaussdb.message.frontend.FrontendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link StartupMessage}.
 */
final class StartupMessageUnitTests {

    @Test
    void constructorNoUsername() {
            assertThatIllegalArgumentException().isThrownBy(() -> new StartupMessage("test-database", null, null))
            .withMessage("username must not be null");
    }

    @Test
    void encode() {
            assertThat(new StartupMessage("test-database", "test-username", new TestStartupParameterProvider())).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> {
                buffer
                    .writeInt(156 + ZoneId.systemDefault().toString().length())
                    .writeInt(196659); // 3， 51

                buffer.writeCharSequence("user", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-username", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("database", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-database", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("application_name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-application-name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("client_encoding", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("utf8", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("DateStyle", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("ISO", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("extra_float_digits", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("2", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("TimeZone", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence(ZoneId.systemDefault().toString(), UTF_8);
                buffer.writeByte(0);

                buffer.writeByte(0);

                return buffer;
            });
    }

    @Test
    void encodeNoDatabase() {
            assertThat(new StartupMessage(null, "test-username", new TestStartupParameterProvider())).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> {
                buffer
                    .writeInt(133 + ZoneId.systemDefault().toString().length())
                    .writeInt(196659);

                buffer.writeCharSequence("user", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-username", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("application_name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-application-name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("client_encoding", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("utf8", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("DateStyle", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("ISO", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("extra_float_digits", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("2", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("TimeZone", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence(ZoneId.systemDefault().toString(), UTF_8);
                buffer.writeByte(0);

                buffer.writeByte(0);

                return buffer;
            });
    }

}
