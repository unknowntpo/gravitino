/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;

public interface AuthorizationRole {
  Role createRole(String name) throws UnsupportedOperationException;

  Role loadRole(String name) throws UnsupportedOperationException;

  boolean dropRole(Role role) throws UnsupportedOperationException;

  boolean toUser(String userName) throws UnsupportedOperationException;

  boolean toGroup(String userName) throws UnsupportedOperationException;

  Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException;
}
