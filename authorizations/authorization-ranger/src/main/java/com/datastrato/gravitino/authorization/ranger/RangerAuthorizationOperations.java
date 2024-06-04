/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationRole;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class RangerAuthorizationOperations implements AuthorizationOperations, AuthorizationRole {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationOperations.class);

  private static RangerClient rangerClient;
  private String rangerUrl = "http://localhost:6080";
  private static final String username = "admin";
  // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
  private static final String password = "rangerR0cks!";
  /* for kerberos authentication:
  authType = "kerberos"
  username = principal
  password = path of the keytab file */
  private static final String authType = "simple";

  String serviceName = "hivedev";
  String hiveType = "hive";

  // Label of the policy that is delegate Gravitino management
  private static final String MANAGED_BY_GRAVITINO = "MANAGED_BY_GRAVITINO";

  private static final String POLICY_ITEM_DEF_USER = "{OWNER}";

  /** Mapping Gravitino privilege name to the underlying authorization system. */
  Map<Privilege.Name, String> mapPrivileges = null;

  public RangerAuthorizationOperations() {
    mapPrivileges = ImmutableMap.<Privilege.Name, String>builder()
            .put(Privilege.Name.TABULAR_SELECT, "select")
            .put(Privilege.Name.TABULAR_UPDATE, "update")
            .put(Privilege.Name.TABULAR_CREATE, "create")
            .put(Privilege.Name.TABULAR_DROP, "drop")
            .put(Privilege.Name.TABULAR_ALTER, "alter")
            .put(Privilege.Name.TABULAR_INDEX, "index")
            .put(Privilege.Name.TABULAR_LOCK, "lock")
            .put(Privilege.Name.TABULAR_READ, "read")
            .put(Privilege.Name.TABULAR_WRITE, "write")
            .put(Privilege.Name.TABULAR_REPL_ADMIN, "repladmin")
            .put(Privilege.Name.TABULAR_SERVICE_ADMIN, "serviceadmin")
            .put(Privilege.Name.TABULAR_ALL, "all")
            .build();
  }

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {
//    String usernameKey = "username";
//    String usernameVal = "admin";
//    String jdbcKey = "jdbc.driverClassName";
//    String jdbcVal = "io.trino.jdbc.TrinoDriver";
//    String jdbcUrlKey = "jdbc.url";
//    String jdbcUrlVal = "http://localhost:8080";

    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);
    LOG.info("Ranger client initialized");
  }

  @Override
  public String translatePrivilege(Privilege.Name name) {
    return mapPrivileges.get(name);
  }

  @Override
  public void close() throws IOException {}

  /**
   * Because Ranger does not support roles, so we will create a policy with the role name as the
   * policy name.
   * 1. Create a policy with the role name as the policy name.
   * 2. Because every Ranger policy must contain a unique resource, we will use a random UUID as
   *    the resource value. You can set real resource values in next steps.
   *    - database: random UUID
   * */
  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    try {
      RangerPolicy policy = new RangerPolicy();
      policy.setService(serviceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("NEW_ROLE-" +  UUID.randomUUID().toString()));
//      policy.setResources(ImmutableMap.of("database", policyResource1));

//      RangerPolicy.RangerPolicyResource policyResource2 = new RangerPolicy.RangerPolicyResource();
//      policyResource2.setValues(Lists.newArrayList("tab1*"));
//      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(ImmutableMap.of("database", policyResource1));


//      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
////      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new RangerPolicy.RangerPolicyItemAccess());
//
//      RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
////      access.setType("select");
//      String serviceType = "hive";
//      RangerServiceDef serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
//      serviceDef.getAccessTypes().forEach(accessType -> {
//        if (accessType.getName().equals("select")) {
////          accessType.getImpliedGrants().forEach(access::setType);
//          access.setType(accessType.getName());
//        }
//      });
//
////      List<String> users = getList("user");
////      List<String> groups = getList("group");
////      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new RangerPolicy.RangerPolicyItemCondition());
//
////      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
//      policyItem.setAccesses(Lists.newArrayList(access));
//
//      policyItem.setUsers(Lists.newArrayList("user"));
//      policyItem.setGroups(Lists.newArrayList("group"));
//      policyItem.setConditions(Lists.newArrayList(new RangerPolicy.RangerPolicyItemCondition(RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME, Collections.singletonList("TAG.attr1 == 'value1'"))));
//
//      policy.setPolicyItems(Lists.newArrayList(policyItem));

      rangerClient.createPolicy(policy);
    } catch (Exception e) {
        throw new RuntimeException(e);
    }

    final AuditInfo auditInfo =
          AuditInfo.builder().withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                  .withCreateTime(Instant.now()).build();
    RoleEntity role =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(name)
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList())
                    .withProperties(ImmutableMap.of())
                    .build();
    return role;
  }

  @Override
  public Role loadRole(String name) throws UnsupportedOperationException {
    return null;
  }

  public Role createRole2(String name) throws UnsupportedOperationException {
    try {
//      RangerService rangerService = rangerClient.getService(serviceName);//.getService(serviceName);
//      LOG.info(rangerService.getName());

//      RangerPolicy policy2 = rangerClient.getPolicy(15);
//      LOG.info(policy2.getName());

      RangerPolicy policy = new RangerPolicy();
      policy.setService(serviceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("default"));
//      policy.setResources(ImmutableMap.of("database", policyResource1));

      RangerPolicy.RangerPolicyResource policyResource2 = new RangerPolicy.RangerPolicyResource();
      policyResource2.setValues(Lists.newArrayList("tab1*"));
//      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(ImmutableMap.of("database", policyResource1, "table", policyResource2, "column", policyResource3));


      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
//      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new RangerPolicy.RangerPolicyItemAccess());

      RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
//      access.setType("select");
      String serviceType = "hive";
      RangerServiceDef serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
      serviceDef.getAccessTypes().forEach(accessType -> {
        if (accessType.getName().equals("select")) {
//          accessType.getImpliedGrants().forEach(access::setType);
          access.setType(accessType.getName());
        }
      });

//      List<String> users = getList("user");
//      List<String> groups = getList("group");
//      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new RangerPolicy.RangerPolicyItemCondition());

//      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
      policyItem.setAccesses(Lists.newArrayList(access));

      policyItem.setUsers(Lists.newArrayList("user"));
      policyItem.setGroups(Lists.newArrayList("group"));
      policyItem.setConditions(Lists.newArrayList(new RangerPolicy.RangerPolicyItemCondition(RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME, Collections.singletonList("TAG.attr1 == 'value1'"))));

      policy.setPolicyItems(Lists.newArrayList(policyItem));

      rangerClient.createPolicy(policy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final AuditInfo auditInfo =
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");
    RoleEntity role =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(name)
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList())
                    .withProperties(map)
                    .build();
    return role;
  }

  /**
   * If the role's object privilege is exist, then return exception.
   * If the role's object privilege is not exist, then return true.
   * */
  @Override
  public boolean dropRole(Role role) throws UnsupportedOperationException {
    // Check whether the role have object privilege
    Iterator<SecurableObject> iter = role.securableObjects().iterator();
    while(iter.hasNext()) {
      SecurableObject securableObject = iter.next();

      RangerPolicy policy = findManagedPolicy(securableObject);
      if (policy != null) {
        LOG.warn("The role({}) have object privilege({}) is exist!", role.name(), securableObject.fullName());
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean toUser(String userName) throws UnsupportedOperationException {
    return false;
  }

  @Override
  public boolean toGroup(String userName) throws UnsupportedOperationException {
    return false;
  }

  @Override
  public Role updateRole(String roleName, RoleChange... changes) throws UnsupportedOperationException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.RenameRole) {
        doRenameRole((RoleChange.RenameRole) change);
      } else if (change instanceof RoleChange.AddSecurableObject) {
        doAddSecurableObject(roleName, (RoleChange.AddSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
                "Unsupported role change type: "
                        + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }
    return null;
  }

  public void createHiveDev(String name) throws RangerServiceException {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "http://localhost:8080";

    final String hiveType = "hive";
    final String serviceName = "hivedev";

    RangerService service = new RangerService();
    service.setType(hiveType);
    service.setName(serviceName);
    service.setConfigs(
            ImmutableMap.<String, String>builder()
                    .put(usernameKey, usernameVal)
                    .put(jdbcKey, jdbcVal)
                    .put(jdbcUrlKey, jdbcUrlVal)
                    .build());

//    RangerService createdService = rangerClient.createService(service);
  }

  private boolean doRenameRole(RoleChange.RenameRole change) {
    return true;
  }

  private boolean doAddSecurableObject(String roleName, RoleChange.AddSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy == null) {
      policy = new RangerPolicy();
      policy.setService(serviceName);
      policy.setName(roleName);
      policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

      List<String> objects = SecurableObjects.DOT.splitToList(change.getSecurableObject().fullName());
      if (objects.size() != 3) {
        throw new RuntimeException("The securable object than 3");
      }

      for (int i = 0; i < objects.size(); i++) {
        RangerPolicy.RangerPolicyResource policyResource = new RangerPolicy.RangerPolicyResource(objects.get(i));
        policy.getResources().put(i==0?"database":i==1?"table":"column", policyResource);
      }
    }

    Map<String, RangerPolicy.RangerPolicyItemAccess> mapAccesses = new HashMap<>();
    policy.getPolicyItems().forEach(policyItem -> {
      policyItem.getAccesses().forEach(access -> {
        // e.g., `select` -> `RangerPolicyItemAccess`
        mapAccesses.put(access.getType(), access);
      });
    });

    RangerPolicy finalPolicy = policy;
    change.getSecurableObject().privileges().forEach(privilege -> {
      if (!mapAccesses.containsKey(translatePrivilege(privilege.name()))) {
        RangerPolicy.RangerPolicyItem newPolicyItem = new RangerPolicy.RangerPolicyItem();
        // Create a new access item for the privilege
        RangerPolicy.RangerPolicyItemAccess newAccess = new RangerPolicy.RangerPolicyItemAccess();
        newAccess.setType(translatePrivilege(privilege.name()));
        newPolicyItem.getAccesses().add(newAccess);
        // Only owner can access the new access item
        newPolicyItem.setUsers(Lists.newArrayList(POLICY_ITEM_DEF_USER));
        // Add the new access item to the policy
        if (Privilege.AccessType.ALLOW == privilege.accessType()) {
          finalPolicy.getPolicyItems().add(newPolicyItem);
        } else {
          finalPolicy.getDenyPolicyItems().add(newPolicyItem);
        }
      } else {
        LOG.info("The access type({}:{}) is exist in the policy({})", privilege.name(), translatePrivilege(privilege.name()), finalPolicy.getName());
      }
    });

    try {
      if (policy.getId() == null) {
        rangerClient.createPolicy(policy);
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
        throw new RuntimeException(e);
    }

    return true;
  }

  private RangerPolicy findManagedPolicy(SecurableObject securableObject) {
    List<String> filterKeys = Lists.newArrayList(RangerRef.SEARCH_FILTER_DATABASE, RangerRef.SEARCH_FILTER_TABLE, RangerRef.SEARCH_FILTER_COLUMN);
    List<String> objects = SecurableObjects.DOT.splitToList(securableObject.fullName());
    if (objects.size() > filterKeys.size()) {
      throw new RuntimeException("The securable object than " + filterKeys.size());
    }

    Map<String,String> policyFilter = new HashMap<>();
    for (int i = 0; i < objects.size(); i++) {
      policyFilter.put(filterKeys.get(i), objects.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);

      List<RangerPolicy> unManagementPolicies = policies.stream().filter(policy -> policy.getPolicyLabels().contains(MANAGED_BY_GRAVITINO) && policy.getIsEnabled()).collect(Collectors.toList());
      if (!unManagementPolicies.isEmpty()) {
          throw new RuntimeException("Exist Gravitino un-management enable policies.");
      }

      // Only return the policies that are delegate gravitino management
      List<RangerPolicy> gravitinoPolicies = policies.stream().filter(policy -> !policy.getPolicyLabels().contains(MANAGED_BY_GRAVITINO) && policy.getIsEnabled()).collect(Collectors.toList());
      if (gravitinoPolicies.size() > 1) {
        throw new RuntimeException("Every object only one Gravitino management enable policies.");
      }

      // Didn't contain duplicate access type in the delegate Gravitino management policy
      gravitinoPolicies.forEach(policy -> {
        policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);
      });

      return gravitinoPolicies.size()==1 ? gravitinoPolicies.get(0) : null;
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  void checkPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem) {
    if (policyItem.getAccesses().size() != 1) {
      throw new RuntimeException("The access type only have one in the delegate Gravitino management policy " + policyItem.getAccesses());
    }
    Map<String, Boolean> mapAccesses = new HashMap<>();
    policyItem.getAccesses().forEach(access -> {
      if (mapAccesses.containsKey(access.getType()) && mapAccesses.get(access.getType())) {
        throw new RuntimeException("Contain duplicate access type in the delegate Gravitino management policy " + access.getType());
      }
      mapAccesses.put(access.getType(), true);
    });
  }
}
