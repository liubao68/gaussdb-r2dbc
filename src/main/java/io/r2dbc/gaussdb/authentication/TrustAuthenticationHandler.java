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

package io.r2dbc.gaussdb.authentication;

import io.r2dbc.gaussdb.message.backend.AuthenticationMessage;
import io.r2dbc.gaussdb.message.frontend.FrontendMessage;
import io.r2dbc.gaussdb.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * An implementation of {@link AuthenticationHandler} that handles implicit trust authentication.
 */
public final class TrustAuthenticationHandler implements AuthenticationHandler {

    /**
     * A static singleton instance that should always be used.
     */
    public static final TrustAuthenticationHandler INSTANCE = new TrustAuthenticationHandler();

    private TrustAuthenticationHandler() {
    }

    @Override
    public FrontendMessage handle(@Nullable AuthenticationMessage message) {
        Assert.requireNonNull(message, "message must not be null");

        throw new IllegalArgumentException("Trust does not require a response");
    }

}
