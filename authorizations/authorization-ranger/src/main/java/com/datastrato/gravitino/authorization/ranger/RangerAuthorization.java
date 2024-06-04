/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.Authorization;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationProvider;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import com.datastrato.gravitino.authorization.Privilege;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/** Implementation of a Mysql catalog in Gravitino. */
public class RangerAuthorization extends BaseAuthorization<RangerAuthorization> {

  RangerAuthorization() {}

  @Override
  public String shortName() {
    return "ranger";
  }

  @Override
  protected AuthorizationOperations newOps(Map<String, String> config) {
    return new RangerAuthorizationOperations();
  }
}
