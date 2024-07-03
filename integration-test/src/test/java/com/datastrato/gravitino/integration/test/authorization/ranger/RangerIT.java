/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.authorization.ranger;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.container.TrinoContainer;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerIT.class);
  protected static final String RANGER_TRINO_REPO_NAME = "trinoDev";
  private static final String RANGER_TRINO_TYPE = "trino";
  protected static final String RANGER_HIVE_REPO_NAME = "hiveDev";
  private static final String RANGER_HIVE_TYPE = "hive";
  protected static final String RANGER_HDFS_REPO_NAME = "hdfsDev";
  private static final String RANGER_HDFS_TYPE = "hdfs";
  private static RangerClient rangerClient;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() {
    containerSuite.startRangerContainer();
    rangerClient = containerSuite.getRangerContainer().rangerClient;
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    try {
      if (rangerClient != null) {
        if (rangerClient.getService(RANGER_TRINO_REPO_NAME) != null) {
          rangerClient.deleteService(RANGER_TRINO_REPO_NAME);
        }
        if (rangerClient.getService(RANGER_HIVE_REPO_NAME) != null) {
          rangerClient.deleteService(RANGER_HIVE_REPO_NAME);
        }
      }
    } catch (RangerServiceException e) {
      // ignore
    }
  }

  public void createRangerTrinoRepository(String tirnoIp) {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = String.format("http:hive2://%s:%d", tirnoIp, TrinoContainer.TRINO_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_TRINO_TYPE);
    service.setName(RANGER_TRINO_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(RangerRef.SEARCH_FILTER_SERVICE_NAME, RANGER_TRINO_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(services.get(0).getType(), RANGER_TRINO_TYPE);
      Assertions.assertEquals(services.get(0).getName(), RANGER_TRINO_REPO_NAME);
      Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createRangerHiveRepository(String hiveIp, boolean cleanAllPolicy) {
    try {
      if (null != rangerClient.getService(RANGER_HIVE_REPO_NAME)) {
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
    String jdbcUrlVal =
        String.format("jdbc:hive2://%s:%d", hiveIp, HiveContainer.HIVE_SERVICE_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_HIVE_TYPE);
    service.setName(RANGER_HIVE_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(RangerRef.SEARCH_FILTER_SERVICE_NAME, RANGER_HIVE_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(services.get(0).getType(), RANGER_HIVE_TYPE);
      Assertions.assertEquals(services.get(0).getName(), RANGER_HIVE_REPO_NAME);
      Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);

      if (cleanAllPolicy) {
        cleanAllPolicy(RANGER_HIVE_REPO_NAME);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createRangerHdfsRepository(String hdfsIp, boolean cleanAllPolicy) {
    try {
      if (null != rangerClient.getService(RANGER_HDFS_REPO_NAME)) {
        return;
      }
    } catch (RangerServiceException e) {
      LOG.error("Error while fetching service: {}", e.getMessage());
    }

    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String authenticationKey = "hadoop.security.authentication";
    String authenticationVal = "simple";
    String protectionKey = "hadoop.rpc.protection";
    String protectionVal = "authentication";
    String authorizationKey = "hadoop.security.authorization";
    String authorizationVal = "false";
    String fsDefaultNameKey = "fs.default.name";
    String fsDefaultNameVal =
        String.format("hdfs://%s:%d", hdfsIp, HiveContainer.HDFS_DEFAULTFS_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_HDFS_TYPE);
    service.setName(RANGER_HDFS_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(authenticationKey, authenticationVal)
            .put(protectionKey, protectionVal)
            .put(authorizationKey, authorizationVal)
            .put(fsDefaultNameKey, fsDefaultNameVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter =
          ImmutableMap.of(RangerRef.SEARCH_FILTER_SERVICE_NAME, RANGER_HDFS_REPO_NAME);
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(services.get(0).getType(), RANGER_HDFS_TYPE);
      Assertions.assertEquals(services.get(0).getName(), RANGER_HDFS_REPO_NAME);
      Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
      Assertions.assertEquals(
          services.get(0).getConfigs().get(authenticationKey), authenticationVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(protectionKey), protectionVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(authorizationKey), authorizationVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(fsDefaultNameKey), fsDefaultNameVal);

      if (cleanAllPolicy) {
        cleanAllPolicy(RANGER_HDFS_REPO_NAME);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  protected void createRangerHivePolicy(
      String policyName,
      Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap,
      List<RangerPolicy.RangerPolicyItem> policyItems) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(RANGER_HIVE_REPO_NAME);
    policy.setName(policyName);
    policy.setResources(policyResourceMap);
    policy.setPolicyItems(policyItems);

    try {
      rangerClient.createPolicy(policy);
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  protected static String updateOrCreateRangerPolicy(
      String type,
      String serviceName,
      String policyName,
      Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap,
      List<RangerPolicy.RangerPolicyItem> policyItems) {
    String retPolicyName = policyName;

    Map<String, String> resourceFilter = new HashMap<>(); // use to match the precise policy
    Map<String, String> policyFilter = new HashMap<>();
    policyFilter.put(RangerRef.SEARCH_FILTER_SERVICE_NAME, serviceName);
    final int[] index = {0};
    policyResourceMap.forEach(
        (k, v) -> {
          if (type.equals(RANGER_HIVE_TYPE)) {
            if (index[0] == 0) {
              policyFilter.put(RangerRef.SEARCH_FILTER_DATABASE, v.getValues().get(0));
              resourceFilter.put(RangerRef.RESOURCE_DATABASE, v.getValues().get(0));
            } else if (index[0] == 1) {
              policyFilter.put(RangerRef.SEARCH_FILTER_TABLE, v.getValues().get(0));
              resourceFilter.put(RangerRef.RESOURCE_TABLE, v.getValues().get(0));
            } else if (index[0] == 2) {
              policyFilter.put(RangerRef.SEARCH_FILTER_COLUMN, v.getValues().get(0));
              resourceFilter.put(RangerRef.RESOURCE_TABLE, v.getValues().get(0));
            }
            index[0]++;
          } else if (type.equals(RANGER_HDFS_TYPE)) {
            policyFilter.put(RangerRef.SEARCH_FILTER_PATH, v.getValues().get(0));
            resourceFilter.put(RangerRef.RESOURCE_PATH, v.getValues().get(0));
          }
        });
    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);
      if (!policies.isEmpty()) {
        // Because Ranger user the wildcard filter, Ranger will return the policy meets
        // the wildcard(*,?) conditions, just like `*.*.*` policy will match `db1.table1.column1`
        // So we need to manual precise filter the policies.
        policies =
            policies.stream()
                .filter(
                    policy ->
                        policy.getResources().entrySet().stream()
                            .allMatch(
                                entry ->
                                    resourceFilter.containsKey(entry.getKey())
                                        && entry.getValue().getValues().size() == 1
                                        && entry
                                            .getValue()
                                            .getValues()
                                            .contains(resourceFilter.get(entry.getKey()))))
                .collect(Collectors.toList());
      }

      Assertions.assertTrue(policies.size() <= 1);
      if (!policies.isEmpty()) {
        RangerPolicy policy = policies.get(0);
        policy.getPolicyItems().addAll(policyItems);
        rangerClient.updatePolicy(policy.getId(), policy);
        retPolicyName = policy.getName();
      } else {
        RangerPolicy policy = new RangerPolicy();
        policy.setServiceType(type);
        policy.setService(serviceName);
        policy.setName(policyName);
        policy.setResources(policyResourceMap);
        policy.setPolicyItems(policyItems);
        rangerClient.createPolicy(policy);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }

    try {
      Thread.sleep(
          1000); // Sleep for a while to wait for the Hive/HDFS Ranger plugin to be updated policy.
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return retPolicyName;
  }

  /** Clean all policy in the Ranger */
  protected static void cleanAllPolicy(String serviceName) {
    try {
      List<RangerPolicy> policies =
          rangerClient.findPolicies(
              ImmutableMap.of(RangerRef.SEARCH_FILTER_SERVICE_NAME, serviceName));
      for (RangerPolicy policy : policies) {
        rangerClient.deletePolicy(policy.getId());
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }
}
