package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class TestAuthorizationManager {
    private static AuthorizationManager authorizationManager;
    private static Config config;
    @BeforeAll
    public static void setUp() throws Exception {
        AuditInfo auditInfo =
                AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();

        CatalogEntity entity =
                CatalogEntity.builder()
                        .withId(1L)
                        .withName("hive-catalog")
                        .withNamespace(Namespace.of("default"))
                        .withType(Catalog.Type.RELATIONAL)
                        .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test"))
                        .withProvider("hive")
                        .withAuditInfo(auditInfo)
                        .build();

        config = new Config(false) {};
        config.set(Configs.AUTHORIZATION_LOAD_ISOLATED, false);

        authorizationManager = new AuthorizationManager(config, null, null);
        authorizationManager.createAuthorization(entity);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (authorizationManager != null) {
            authorizationManager.close();
        }
    }


    @Test
    public void testCreateAuthorization() {

    }
}
