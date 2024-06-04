/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.datastrato.gravitino.utils.ThrowableFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Manages the authorization instances and operations. */
public class AuthorizationManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationManager.class);

  public static final String AUTHORIZATION_PROVIDER = "AUTHORIZATION_PROVIDER";

  private final Config config;

  public AuthorizationManager(Config config, EntityStore store, IdGenerator idGenerator) {
    this.config = config;
  }

  /** Wrapper class for an Authorization instance and its class loader. */
  public static class AuthorizationWrapper {
    private BaseAuthorization authorization;

    private IsolatedClassLoader classLoader;

    public AuthorizationWrapper(BaseAuthorization auth, IsolatedClassLoader classLoader) {
      this.authorization = auth;
      this.classLoader = classLoader;
    }

    public <R> R doWithTableOps(ThrowableFunction<TableCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTables() == null) {
              throw new UnsupportedOperationException("Catalog does not support table operations");
            }
            return fn.apply(asTables());
          });
    }

    private TableCatalog asTables() {
      return authorization.ops() instanceof TableCatalog
          ? (TableCatalog) authorization.ops()
          : null;
    }
  }

  BaseAuthorization createAuthorization(CatalogEntity entity) {
    return createAuthorizationWrapper(entity).authorization;
  }

  private AuthorizationWrapper createAuthorizationWrapper(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = conf.get("AUTHORIZATION_PROVIDER");

    IsolatedClassLoader classLoader = createClassLoader(provider, conf);
    BaseAuthorization<?> catalog = createBaseAuthorization(classLoader, entity);

    AuthorizationWrapper wrapper = new AuthorizationWrapper(catalog, classLoader);
    // Validate catalog properties and initialize the config
    classLoader.withClassLoader(
        cl -> {
          //              Map<String, String> configWithoutId = Maps.newHashMap(conf);
          //              configWithoutId.remove(ID_KEY);
          //              validatePropertyForCreate(catalog.catalogPropertiesMetadata(),
          // configWithoutId);

          // Call wrapper.catalog.properties() to make BaseCatalog#properties in IsolatedClassLoader
          // not null. Why we do this? Because wrapper.catalog.properties() need to be called in the
          // IsolatedClassLoader, it needs to load the specific catalog class such as HiveCatalog or
          // so. For simply, We will preload the value of properties and thus AppClassLoader can get
          // the value of properties.
          //              wrapper.authorization.properties();
          //              wrapper.authorization.capability();
          return null;
        },
        IllegalArgumentException.class);

    return wrapper;
  }

  private IsolatedClassLoader createClassLoader(String provider, Map<String, String> conf) {
    if (config.get(Configs.AUTHORIZATION_LOAD_ISOLATED)) {
      String pkgPath = buildPkgPath(conf, provider);
      String confPath = buildConfPath(conf, provider);
      return IsolatedClassLoader.buildClassLoader(Lists.newArrayList(pkgPath, confPath));
    } else {
      // This will use the current class loader, it is mainly used for test.
      return new IsolatedClassLoader(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }

  private BaseAuthorization<?> createBaseAuthorization(
      IsolatedClassLoader classLoader, CatalogEntity entity) {
    // Load Catalog class instance
    BaseAuthorization<?> authorization =
        createAuthorizationInstance(classLoader, entity.getProperties().get(AUTHORIZATION_PROVIDER));
    authorization.withAuthorizationConf(entity.getProperties()); // .withCatalogEntity(entity);
    return authorization;
  }

  private BaseAuthorization<?> createAuthorizationInstance(
      IsolatedClassLoader classLoader, String provider) {
    BaseAuthorization<?> authorization;
    try {
      authorization =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends AuthorizationProvider> providerClz =
                      lookupAuthorizationProvider(provider, cl);
                  return (BaseAuthorization) providerClz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                  LOG.error("Failed to load catalog with provider: {}", provider, e);
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      LOG.error("Failed to load catalog with class loader", e);
      throw new RuntimeException(e);
    }

    if (authorization == null) {
      throw new RuntimeException("Failed to load catalog with provider: " + provider);
    }
    return authorization;
  }

  private Class<? extends AuthorizationProvider> lookupAuthorizationProvider(
      String provider, ClassLoader cl) {
    ServiceLoader<AuthorizationProvider> loader =
        ServiceLoader.load(AuthorizationProvider.class, cl);

    List<Class<? extends AuthorizationProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(AuthorizationProvider::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No catalog provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple catalog providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);
    String pkgPath;
    if (pkg != null) {
      pkgPath = String.join(File.separator, pkg, "libs");
    } else if (testEnv) {
      // In test, the catalog package is under the build directory.
      pkgPath =
          String.join(
              File.separator,
              gravitinoHome,
              "authorizations",
              "authorization-" + provider,
              "build",
              "libs");
    } else {
      // In real environment, the catalog package is under the catalog directory.
      pkgPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "libs");
    }

    return pkgPath;
  }

  private String buildConfPath(Map<String, String> properties, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String confPath;
    String pkg = properties.get(Catalog.PROPERTY_PACKAGE);
    if (pkg != null) {
      confPath = String.join(File.separator, pkg, "conf");
    } else if (testEnv) {
      confPath =
          String.join(
              File.separator,
              gravitinoHome,
              "authorizations",
              "authorization-" + provider,
              "build",
              "resources",
              "main");
    } else {
      confPath = String.join(File.separator, gravitinoHome, "authorizations", provider, "conf");
    }
    return confPath;
  }

  @Override
  public void close() throws IOException {}
}
