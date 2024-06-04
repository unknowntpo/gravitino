/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.io.Closeable;
import java.util.Map;

public interface AuthorizationOperations extends Closeable {
  void initialize(Map<String, String> config) throws RuntimeException;

  public String translatePrivilege(Privilege.Name name);
}
