/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.gaussdb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ByteBufUtils}.
 */
final class ByteBufUtilsUnitTests {

    @Test
    void shouldCopyByteBuffer() {

        ByteBuf source = Unpooled.wrappedBuffer("hello-world".getBytes());
        ByteBuffer byteBuffer = ByteBufferUtils.toByteBuffer(source);

        assertThat(byteBuffer).isEqualTo(ByteBuffer.wrap("hello-world".getBytes()));
    }

}
