/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger.integration.test;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.authorization.ranger.RangerAuthorizationOperations;
import com.datastrato.gravitino.authorization.ranger.RangerHttpClient;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.plugin.util.SearchFilter.POLICY_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;

//@Tag("gravitino-docker-it")
//@TestInstance(Lifecycle.PER_CLASS)
public class RangerAuthorizationIT {// extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationIT.class);

  private static final String serviceName = "hivedev";
  private static final String hiveType = "hive";
  private static RangerClient rangerClient;
//  private static final String provider = "ranger";

//  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static String roleName = GravitinoITUtils.genRandomName("testRole");

  static RangerAuthorizationOperations rangerAuthorizationOperations;

  @BeforeAll
  public static void setup() throws RangerServiceException, IOException {
//    containerSuite.startRangerContainer();
//    rangerClient = containerSuite.getRangerContainer().rangerClient;

//    RangerHttpClient client = new RangerHttpClient();
//    Map<String,String> filter = Collections.emptyMap();
//    List<RangerPolicy> policies = client.findPolicies(ImmutableMap.of(SearchFilter.SERVICE_NAME, "hivedev",
//            SearchFilter.POLICY_NAME, "testRole_18a2b0c0",
//            SearchFilter.RESOURCE_PREFIX + "database", "NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62")
//    );

    /**
     * "database" -> {RangerPolicy$RangerPolicyResource@4219} "RangerPolicyResource={values={NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62 } isExcludes={false} isRecursive={false} }"
     *  key = "database"
     *  value = {RangerPolicy$RangerPolicyResource@4219} "RangerPolicyResource={values={NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62 } isExcludes={false} isRecursive={false} }"
     *   values = {ArrayList@4222}  size = 1
     *    0 = "NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62"
     * */
    rangerAuthorizationOperations = new RangerAuthorizationOperations();
    rangerAuthorizationOperations.initialize(ImmutableMap.of("provider", "ranger"));

    initRangerClient();

//    RangerPolicy policy = rangerClient.getPolicy(11);
//    LOG.info("Policy: {}", policy);
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
//    if (rangerClient != null) {
//      rangerClient.deleteService(serviceName);
//    }
  }

  private static void initRangerClient() throws RangerServiceException {
    final String username = "admin";
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    final String password = "rangerR0cks!";
    /* for kerberos authentication:
    authType = "kerberos"
    username = principal
    password = path of the keytab file */
    final String authType = "simple";
    String rangerUrl = String.format("http://localhost:%s", 6080);
    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);

    LOG.info("");

    createHiveService();
  }

  @Test
  public void createRole() {
    rangerAuthorizationOperations.createRole(roleName);
  }

  /**
   * Access Control Syntax: GRANT <privilege_type> ON <securable_object> TO <principal>
   * Access Control Sample: GRANT SELECT ON TABLE <schema-name>.<table-name> TO users
   * */
  @Test
  public void grantPrivilegeToObjectToRole() {
    SecurableObject securableObjectTable = SecurableObjects.ofNamespace(
            SecurableObject.Type.TABLE,
            Namespace.of("db1", "tab1", "column1"),
            Lists.newArrayList(Privileges.TabularSelect.allow()));
    rangerAuthorizationOperations.updateRole(roleName,
            RoleChange.addSecurableObject(securableObjectTable));

  }

  public static void createHiveService() throws RangerServiceException {
    try {
        if(null != rangerClient.getService(serviceName)) {
          return;
        }
    } catch (RangerServiceException e) {
        LOG.error("Error while fetching service: {}", e.getMessage());
    }

    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "jdbc:hive2://172.17.0.2:10000";

    RangerService service = new RangerService();
    service.setType(hiveType);
    service.setName(serviceName);
    service.setConfigs(
            ImmutableMap.<String, String>builder()
                    .put(usernameKey, usernameVal)
                    .put(passwordKey, passwordVal)
                    .put(jdbcKey, jdbcVal)
                    .put(jdbcUrlKey, jdbcUrlVal)
                    .build());

    RangerService createdService = rangerClient.createService(service);
    Assertions.assertNotNull(createdService);

    Map<String, String> filter = Collections.emptyMap();
    List<RangerService> services = rangerClient.findServices(filter);
    Assertions.assertEquals(services.get(0).getName(), serviceName);
    Assertions.assertEquals(services.get(0).getType(), hiveType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }
}
