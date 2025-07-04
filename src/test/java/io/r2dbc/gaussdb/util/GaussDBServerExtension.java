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

package io.r2dbc.gaussdb.util;

import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.gaussdb.GaussDBConnectionConfiguration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GaussDBContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;
import reactor.util.annotation.Nullable;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Supplier;

import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * JUnit Extension to establish a GaussDB database context during integration tests.
 * Uses either {@link TestContainer Testcontainers} or a {@link External locally available database}.
 */
public final class GaussDBServerExtension implements BeforeAllCallback, AfterAllCallback {

    static final String IMAGE_NAME = "opengauss/opengauss:latest";

    static GaussDBContainer<?> containerInstance = null;

    static Network containerNetwork = null;

    private final Supplier<GaussDBContainer<?>> container = () -> {

        if (GaussDBServerExtension.containerInstance != null) {
            return GaussDBServerExtension.containerInstance;
        }

        GaussDBServerExtension.containerNetwork = Network.newNetwork();
        return GaussDBServerExtension.containerInstance = container();
    };

    private final DatabaseContainer gaussdb = getContainer();

    private final boolean useTestContainer = this.gaussdb instanceof TestContainer;

    private HikariDataSource dataSource;

    private JdbcOperations jdbcOperations;

    public GaussDBServerExtension() {
    }

    private DatabaseContainer getContainer() {

        File testrc = new File(".testrc");
        String preference = "testcontainer";
        if (testrc.exists()) {
            Properties properties = new Properties();
            try (FileReader reader = new FileReader(testrc)) {
                properties.load(reader);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            preference = properties.getProperty("preference", preference);
        }

        if (preference.equals(External.PREFERENCE)) {
            return new External();
        }

        return new TestContainer(this.container.get());
    }

    @Override
    public void afterAll(ExtensionContext context) {

        if (this.dataSource != null) {
            this.dataSource.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        initialize();
    }

    public void initialize() {

        if (this.useTestContainer) {
            this.container.get().start();
        }

        initializeConnectors();
    }

    private void initializeConnectors() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername(getUsername());
        dataSource.setPassword(getPassword());
        dataSource.setJdbcUrl(String.format("jdbc:gaussdb://%s:%d/%s?prepareThreshold=1", getHost(), getPort(), getDatabase()));

        this.dataSource = dataSource;
        this.jdbcOperations = new JdbcTemplate(this.dataSource);
    }

    public String getClientCrt() {
        return getResourcePath("client.crt").toAbsolutePath().toString();
    }

    public String getClientKey() {
        return getResourcePath("client.key").toAbsolutePath().toString();
    }

    public GaussDBConnectionConfiguration.Builder configBuilder() {
        return GaussDBConnectionConfiguration.builder().database(getDatabase()).host(getHost()).port(getPort()).username(getUsername()).password(getPassword());
    }

    public GaussDBConnectionConfiguration getConnectionConfiguration() {
        return configBuilder().build();
    }

    public String getDatabase() {
        return this.gaussdb.getDatabase();
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    @Nullable
    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    public String getHost() {
        return this.gaussdb.getHost();
    }

    public int getPort() {
        return this.gaussdb.getPort();
    }

    public String getServerCrt() {
        return getResourcePath("server.crt").toAbsolutePath().toString();
    }

    public String getServerKey() {
        return getResourcePath("server.key").toAbsolutePath().toString();
    }

    public String getUsername() {
        return this.gaussdb.getUsername();
    }

    public String getPassword() {
        return this.gaussdb.getPassword();
    }

    public DatabaseContainer getGaussdb() {
        return this.gaussdb;
    }

    private <T extends GaussDBContainer<T>> T container() {
        T container = new GaussDBContainer<T>(IMAGE_NAME)
            .withNetwork(GaussDBServerExtension.containerNetwork);

        return container;
    }

    private Path getResourcePath(String name) {

        URL resource = getClass().getClassLoader().getResource(name);
        if (resource == null) {
            throw new IllegalStateException("Resource not found: " + name);
        }

        try {
            return Paths.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot convert to path for: " + name, e);
        }
    }

    private MountableFile getHostPath(String name, int mode) {
        return forHostPath(getResourcePath(name), mode);
    }

    /**
     * Interface to be implemented by database providers (provided database, test container).
     */
    interface DatabaseContainer {

        String getHost();

        @Nullable
        Network getNetwork();

        int getPort();

        String getDatabase();

        String getUsername();

        String getPassword();

        String getNetworkAlias();

    }

    /**
     * Externally provided Postgres instance.
     */
    static class External implements DatabaseContainer {

        public static final String PREFERENCE = "external";

        public static final External INSTANCE = new External();

        @Override
        public String getHost() {
            return "localhost";
        }

        @Override
        @Nullable
        public Network getNetwork() {
            return null;
        }

        @Override
        public int getPort() {
            return 8000;
        }

        @Override
        public String getDatabase() {
            return "postgres";
        }

        @Override
        public String getUsername() {
            return GaussDBContainer.DEFAULT_USER_NAME;
        }

        @Override
        public String getPassword() {
            return GaussDBContainer.DEFAULT_PASSWORD;
        }

        @Override
        public String getNetworkAlias() {
            return this.getHost();
        }

    }

    /**
     * {@link DatabaseContainer} provided by {@link JdbcDatabaseContainer}.
     */
    static class TestContainer implements DatabaseContainer {

        public static final String PREFERENCE = "testcontainer";

        private final JdbcDatabaseContainer<?> container;

        TestContainer(JdbcDatabaseContainer<?> container) {
            this.container = container;
        }

        @Override
        public String getHost() {
            return this.container.getHost();
        }

        @Override
        public Network getNetwork() {
            return this.container.getNetwork();
        }

        @Override
        public int getPort() {
            return this.container.getMappedPort(GaussDBContainer.GaussDB_PORT);
        }

        @Override
        public String getDatabase() {
            return this.container.getDatabaseName();
        }

        @Override
        public String getUsername() {
            return this.container.getUsername();
        }

        @Override
        public String getPassword() {
            return this.container.getPassword();
        }

        @Override
        public String getNetworkAlias() {
            return "r2dbc-gaussdb";
        }

    }

}
