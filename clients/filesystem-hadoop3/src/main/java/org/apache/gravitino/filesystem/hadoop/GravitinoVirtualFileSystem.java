/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.filesystem.hadoop;

import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.extractIdentifier;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getConfigMap;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getSubPathFromGvfsPath;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Apache Gravitino server, and creates an independent file system for it to act as an agent for
 * users to access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);

  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private String metalakeName;
  private Cache<NameIdentifier, FilesetCatalog> catalogCache;
  private ScheduledThreadPoolExecutor catalogCleanScheduler;
  // Fileset nameIdentifier-locationName Pair and its corresponding FileSystem cache, the name
  // identifier has four levels, the first level is metalake name.
  private Cache<Pair<NameIdentifier, String>, FileSystem> internalFileSystemCache;
  private ScheduledThreadPoolExecutor internalFileSystemCleanScheduler;
  private long defaultBlockSize;

  private static final String SLASH = "/";
  private final Map<String, FileSystemProvider> fileSystemProvidersMap = Maps.newHashMap();
  private String currentLocationEnvVar;

  @Nullable private String currentLocationName;

  private static final Set<String> CATALOG_NECESSARY_PROPERTIES_TO_KEEP =
      Sets.newHashSet(
          OSSProperties.GRAVITINO_OSS_ENDPOINT,
          OSSProperties.GRAVITINO_OSS_REGION,
          S3Properties.GRAVITINO_S3_ENDPOINT,
          S3Properties.GRAVITINO_S3_REGION,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long evictionMillsAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        evictionMillsAfterAccess > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);

    initializeFileSystemCache(maxCapacity, evictionMillsAfterAccess);
    initializeCatalogCache();

    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    this.client = GravitinoVirtualFileSystemUtils.createClient(configuration);
    // Register the default local and HDFS FileSystemProvider
    fileSystemProvidersMap.putAll(getFileSystemProviders());

    this.currentLocationEnvVar =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT);
    this.currentLocationName = initCurrentLocationName(configuration);

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    this.defaultBlockSize =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_BLOCK_SIZE,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_BLOCK_SIZE_DEFAULT);

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  Cache<Pair<NameIdentifier, String>, FileSystem> internalFileSystemCache() {
    return internalFileSystemCache;
  }

  @VisibleForTesting
  FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String virtualPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    // if the storage location ends with "/",
    // we should truncate this to avoid replace issues.
    Path path =
        new Path(
            filePath.replaceFirst(
                actualPrefix.endsWith(SLASH) && !virtualPrefix.endsWith(SLASH)
                    ? actualPrefix.substring(0, actualPrefix.length() - 1)
                    : actualPrefix,
                virtualPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    Optional<FilesetContextPair> context =
        getFilesetContext(newDir, FilesetDataOperation.SET_WORKING_DIR);
    try {
      throwFilesetPathNotFoundExceptionIf(
          () -> !context.isPresent(), newDir, FilesetDataOperation.SET_WORKING_DIR);
    } catch (FilesetPathNotFoundException e) {
      throw new RuntimeException(e);
    }

    context.get().getFileSystem().setWorkingDirectory(context.get().getActualFileLocation());
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    Optional<FilesetContextPair> context = getFilesetContext(path, FilesetDataOperation.OPEN);
    throwFilesetPathNotFoundExceptionIf(
        () -> !context.isPresent(), path, FilesetDataOperation.OPEN);
    return context.get().getFileSystem().open(context.get().getActualFileLocation(), bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    Optional<FilesetContextPair> context = getFilesetContext(path, FilesetDataOperation.CREATE);
    if (!context.isPresent()) {
      throw new IOException(
          "Fileset is not found for path: "
              + path
              + " for operation CREATE. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.");
    }

    return context
        .get()
        .getFileSystem()
        .create(
            context.get().getActualFileLocation(),
            permission,
            overwrite,
            bufferSize,
            replication,
            blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    Optional<FilesetContextPair> context = getFilesetContext(path, FilesetDataOperation.APPEND);
    throwFilesetPathNotFoundExceptionIf(
        () -> !context.isPresent(), path, FilesetDataOperation.APPEND);
    return context
        .get()
        .getFileSystem()
        .append(context.get().getActualFileLocation(), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    NameIdentifier srcIdentifier = extractIdentifier(metalakeName, src.toString());
    NameIdentifier dstIdentifier = extractIdentifier(metalakeName, dst.toString());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path "
            + "fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    Optional<FilesetContextPair> srcContext = getFilesetContext(src, FilesetDataOperation.RENAME);
    throwFilesetPathNotFoundExceptionIf(
        () -> !srcContext.isPresent(), src, FilesetDataOperation.RENAME);

    Optional<FilesetContextPair> dstContext = getFilesetContext(dst, FilesetDataOperation.RENAME);

    // Because src context and dst context are the same, so if src context is present, dst context
    // must be present.
    return srcContext
        .get()
        .getFileSystem()
        .rename(srcContext.get().getActualFileLocation(), dstContext.get().getActualFileLocation());
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    Optional<FilesetContextPair> context = getFilesetContext(path, FilesetDataOperation.DELETE);
    if (context.isPresent()) {
      return context.get().getFileSystem().delete(context.get().getActualFileLocation(), recursive);
    } else {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Optional<FilesetContextPair> context =
        getFilesetContext(path, FilesetDataOperation.GET_FILE_STATUS);
    throwFilesetPathNotFoundExceptionIf(
        () -> !context.isPresent(), path, FilesetDataOperation.GET_FILE_STATUS);

    FileStatus fileStatus =
        context.get().getFileSystem().getFileStatus(context.get().getActualFileLocation());
    NameIdentifier identifier = extractIdentifier(metalakeName, path.toString());
    String subPath = getSubPathFromGvfsPath(identifier, path.toString());
    String storageLocation =
        context
            .get()
            .getActualFileLocation()
            .toString()
            .substring(
                0, context.get().getActualFileLocation().toString().length() - subPath.length());
    return convertFileStatusPathPrefix(
        fileStatus, storageLocation, getVirtualLocation(identifier, true));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    Optional<FilesetContextPair> context =
        getFilesetContext(path, FilesetDataOperation.LIST_STATUS);
    throwFilesetPathNotFoundExceptionIf(
        () -> !context.isPresent(), path, FilesetDataOperation.LIST_STATUS);

    FileStatus[] fileStatusResults =
        context.get().getFileSystem().listStatus(context.get().getActualFileLocation());
    NameIdentifier identifier = extractIdentifier(metalakeName, path.toString());
    String subPath = getSubPathFromGvfsPath(identifier, path.toString());
    String storageLocation =
        context
            .get()
            .getActualFileLocation()
            .toString()
            .substring(
                0, context.get().getActualFileLocation().toString().length() - subPath.length());
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus, storageLocation, getVirtualLocation(identifier, true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Optional<FilesetContextPair> context = getFilesetContext(path, FilesetDataOperation.MKDIRS);
    if (!context.isPresent()) {
      throw new IOException(
          "Fileset is not found for path: "
              + path
              + " for operation MKDIRS. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.");
    }

    return context.get().getFileSystem().mkdirs(context.get().getActualFileLocation(), permission);
  }

  @Override
  public short getDefaultReplication(Path f) {
    Optional<FilesetContextPair> context =
        getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_REPLICATION);
    return context
        .map(c -> c.getFileSystem().getDefaultReplication(c.getActualFileLocation()))
        .orElse((short) 1);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    Optional<FilesetContextPair> context =
        getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
    return context
        .map(c -> c.getFileSystem().getDefaultBlockSize(c.getActualFileLocation()))
        .orElse(defaultBlockSize);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
    List<Token<?>> tokenList = Lists.newArrayList();
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        tokenList.addAll(Arrays.asList(fileSystem.addDelegationTokens(renewer, credentials)));
      } catch (IOException e) {
        LOG.warn("Failed to add delegation tokens for filesystem: {}", fileSystem.getUri(), e);
      }
    }
    return tokenList.stream().distinct().toArray(Token[]::new);
  }

  @Override
  public synchronized void close() throws IOException {
    // close all actual FileSystems
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        fileSystem.close();
      } catch (IOException e) {
        // ignore
      }
    }
    internalFileSystemCache.invalidateAll();
    catalogCache.invalidateAll();
    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    catalogCleanScheduler.shutdownNow();
    internalFileSystemCleanScheduler.shutdownNow();
    super.close();
  }

  private void initializeFileSystemCache(int maxCapacity, long expireAfterAccess) {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.internalFileSystemCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-filesystem-cache-cleaner"));
    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .scheduler(Scheduler.forScheduledExecutorService(internalFileSystemCleanScheduler))
            .removalListener(
                (key, value, cause) -> {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    try {
                      fs.close();
                    } catch (IOException e) {
                      LOG.error("Cannot close the file system for fileset: {}", key, e);
                    }
                  }
                });
    if (expireAfterAccess > 0) {
      cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }
    this.internalFileSystemCache = cacheBuilder.build();
  }

  private void initializeCatalogCache() {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-catalog-cache-cleaner"));
    // In most scenarios, it will not read so many catalog filesets at the same time, so we can just
    // set a default value for this cache.
    this.catalogCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
            .build();
  }

  private String initCurrentLocationName(Configuration configuration) {
    // get from configuration first, otherwise use the env variable
    // if both are not set, return null which means use the default location
    return Optional.ofNullable(configuration.get(FS_GRAVITINO_CURRENT_LOCATION_NAME))
        .orElse(System.getenv(currentLocationEnvVar));
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  private String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  private Optional<FilesetContextPair> getFilesetContext(
      Path virtualPath, FilesetDataOperation operation) {
    NameIdentifier identifier = extractIdentifier(metalakeName, virtualPath.toString());
    String virtualPathString = virtualPath.toString();
    String subPath = getSubPathFromGvfsPath(identifier, virtualPathString);
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, identifier.namespace().level(1));

    FilesetCatalog filesetCatalog;
    try {
      filesetCatalog =
          catalogCache.get(
              catalogIdent, ident -> client.loadCatalog(catalogIdent.name()).asFilesetCatalog());
      Preconditions.checkArgument(
          filesetCatalog != null,
          String.format("Loaded fileset catalog: %s is null.", catalogIdent));
    } catch (NoSuchCatalogException | CatalogNotInUseException e) {
      LOG.warn("Cannot get fileset catalog by identifier: {}", catalogIdent, e);
      return Optional.empty();
    }

    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put(
        FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
        InternalClientType.HADOOP_GVFS.name());
    contextMap.put(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION, operation.name());
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
    CallerContext.CallerContextHolder.set(callerContext);

    String actualFileLocation;
    try {
      actualFileLocation =
          filesetCatalog.getFileLocation(
              NameIdentifier.of(identifier.namespace().level(2), identifier.name()),
              subPath,
              currentLocationName);
    } catch (NoSuchFilesetException e) {
      LOG.warn("Cannot get file location by identifier: {}, sub_path {}", identifier, subPath, e);
      return Optional.empty();
    }

    Path filePath = new Path(actualFileLocation);
    URI uri = filePath.toUri();
    // we cache the fs for the same scheme, so we can reuse it
    String scheme = uri.getScheme();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(scheme), "Scheme of the actual file location cannot be null.");
    FileSystem fs =
        internalFileSystemCache.get(
            Pair.of(identifier, currentLocationName),
            ident -> {
              try {
                FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
                if (provider == null) {
                  throw new GravitinoRuntimeException(
                      "Unsupported file system scheme: %s for %s.",
                      scheme, GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME);
                }

                // Reset the FileSystem service loader to make sure the FileSystem will reload the
                // service file systems, this is a temporary solution to fix the issue
                // https://github.com/apache/gravitino/issues/5609
                resetFileSystemServiceLoader(scheme);

                Catalog catalog = (Catalog) filesetCatalog;
                Map<String, String> necessaryPropertyFromCatalog =
                    catalog.properties().entrySet().stream()
                        .filter(
                            property ->
                                CATALOG_NECESSARY_PROPERTIES_TO_KEEP.contains(property.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                Map<String, String> totalProperty = Maps.newHashMap(necessaryPropertyFromCatalog);
                totalProperty.putAll(getConfigMap(getConf()));
                totalProperty.putAll(getCredentialProperties(provider, catalog, identifier));
                return provider.getFileSystem(filePath, totalProperty);

              } catch (IOException ioe) {
                throw new GravitinoRuntimeException(
                    ioe,
                    "Exception occurs when create new FileSystem for actual uri: %s, msg: %s",
                    uri,
                    ioe.getMessage());
              }
            });

    return Optional.of(new FilesetContextPair(new Path(actualFileLocation), fs));
  }

  private Map<String, String> getCredentialProperties(
      FileSystemProvider fileSystemProvider, Catalog catalog, NameIdentifier filesetIdentifier) {
    // Do not support credential vending, we do not need to add any credential properties.
    if (!(fileSystemProvider instanceof SupportsCredentialVending)) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    try {
      Fileset fileset =
          catalog
              .asFilesetCatalog()
              .loadFileset(
                  NameIdentifier.of(
                      filesetIdentifier.namespace().level(2), filesetIdentifier.name()));
      Credential[] credentials = fileset.supportsCredentials().getCredentials();
      if (credentials.length > 0) {
        mapBuilder.put(
            GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
            DefaultGravitinoFileSystemCredentialsProvider.class.getCanonicalName());
        mapBuilder.put(
            GravitinoFileSystemCredentialsProvider.GVFS_NAME_IDENTIFIER,
            filesetIdentifier.toString());

        SupportsCredentialVending supportsCredentialVending =
            (SupportsCredentialVending) fileSystemProvider;
        mapBuilder.putAll(supportsCredentialVending.getFileSystemCredentialConf(credentials));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return mapBuilder.build();
  }

  private void resetFileSystemServiceLoader(String fsScheme) {
    try {
      Map<String, Class<? extends FileSystem>> serviceFileSystems =
          (Map<String, Class<? extends FileSystem>>)
              FieldUtils.getField(FileSystem.class, "SERVICE_FILE_SYSTEMS", true).get(null);

      if (serviceFileSystems.containsKey(fsScheme)) {
        return;
      }

      // Set this value to false so that FileSystem will reload the service file systems when
      // needed.
      FieldUtils.getField(FileSystem.class, "FILE_SYSTEMS_LOADED", true).set(null, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, FileSystemProvider> getFileSystemProviders() {
    Map<String, FileSystemProvider> resultMap = Maps.newHashMap();
    ServiceLoader<FileSystemProvider> allFileSystemProviders =
        ServiceLoader.load(FileSystemProvider.class);

    Streams.stream(allFileSystemProviders.iterator())
        .forEach(
            fileSystemProvider -> {
              if (resultMap.containsKey(fileSystemProvider.scheme())) {
                throw new UnsupportedOperationException(
                    String.format(
                        "File system provider: '%s' with scheme '%s' already exists in the provider list, "
                            + "please make sure the file system provider scheme is unique.",
                        fileSystemProvider.getClass().getName(), fileSystemProvider.scheme()));
              }
              resultMap.put(fileSystemProvider.scheme(), fileSystemProvider);
            });
    return resultMap;
  }

  private void throwFilesetPathNotFoundExceptionIf(
      Supplier<Boolean> condition, Path path, FilesetDataOperation op)
      throws FilesetPathNotFoundException {
    if (condition.get()) {
      throw new FilesetPathNotFoundException(
          String.format(
              "Path [%s] not found for operation [%s] because of fileset and related "
                  + "metadata not existed in Gravitino",
              path, op));
    }
  }

  private static class FilesetContextPair {
    private final Path actualFileLocation;
    private final FileSystem fileSystem;

    public FilesetContextPair(Path actualFileLocation, FileSystem fileSystem) {
      this.actualFileLocation = actualFileLocation;
      this.fileSystem = fileSystem;
    }

    public Path getActualFileLocation() {
      return actualFileLocation;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }
  }
}
