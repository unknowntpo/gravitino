/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAuthorization<T extends BaseAuthorization>
        implements Authorization, AuthorizationProvider, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseAuthorization.class);

  private volatile AuthorizationOperations ops = null;

  private Map<String, String> conf;

  /** Mapping Gravitino privilege name to the underlying authorization system. */
  protected Map<Privilege.Name, String> mapPrivileges;

  public String underlayPrivilege(Privilege.Name name) {
    return mapPrivileges.get(name);
  }

  public T withAuthorizationConf(Map<String, String> conf) {
    this.conf = conf;
    return (T) this;
  }

  protected abstract AuthorizationOperations newOps(Map<String, String> config);

  public AuthorizationOperations ops() {
    LOG.info("");
    if (ops == null) {
      synchronized (this) {
        if (ops == null) {
          //          Preconditions.checkArgument(
          //                  entity != null && conf != null, "entity and conf must be set before
          // calling ops()");
          ops = newOps(conf);
          ops.initialize(conf);
        }
      }
    }

    return ops;
  }

  @Override
  public void close() throws IOException {
    if (ops != null) {
      ops.close();
      ops = null;
    }
  }
}
