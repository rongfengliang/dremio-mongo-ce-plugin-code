package com.dremio.plugins.mongo.connection;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.plugins.mongo.MongoConf;
import com.dremio.plugins.mongo.metadata.MongoCollections;
import com.dremio.plugins.mongo.metadata.MongoTopology;
import com.dremio.plugins.mongo.metadata.MongoVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoConnectionManager implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(MongoConnectionManager.class);
   private final Cache<MongoConnectionKey, MongoClient> addressClientMap;
   private final MongoClientURI clientURI;
   private final MongoClientOptions clientOptions;
   private final String authDatabase;
   private final String user;
   private final String pluginName;
   private final boolean canReadOnlyOnSecondary;
   private final long authTimeoutInMillis;
   private final long readTimeoutInMillis;

   public MongoConnectionManager(MongoConf config, String pluginName) {
      StringBuilder connection = new StringBuilder("mongodb://");
      if (AuthenticationType.MASTER.equals(config.authenticationType)) {
         String username = config.username;
         if (username != null) {
            connection.append(urlEncode(username));
         }

         String password = config.password;
         if (password != null) {
            connection.append(":").append(urlEncode(password));
         }

         connection.append("@");
         appendHosts(connection, (Iterable)Preconditions.checkNotNull(config.hostList, "hostList missing"), ',').append("/");
         String database = config.authDatabase;
         if (database != null) {
            connection.append(database);
         }
      } else {
         appendHosts(connection, (Iterable)Preconditions.checkNotNull(config.hostList, "hostList missing"), ',').append("/");
      }

      connection.append("?");
      List<Property> properties = new ArrayList();
      if (config.propertyList != null) {
         properties.addAll(config.propertyList);
      }

      if (config.useSsl) {
         properties.add(new Property("ssl", "true"));
      }

      connection.append(FluentIterable.from(properties).transform(new Function<Property, String>() {
         public String apply(Property input) {
            return input.value != null ? input.name + "=" + input.value : input.name;
         }
      }).join(Joiner.on('&')));
      this.pluginName = pluginName;
      this.authTimeoutInMillis = (long)config.authenticationTimeoutMillis;
      this.canReadOnlyOnSecondary = config.secondaryReadsOnly;
      this.clientURI = new MongoClientURI(connection.toString());
      if (this.clientURI.getHosts().isEmpty()) {
         throw UserException.dataReadError().message("Unable to configure Mongo with empty host list.", new Object[0]).build(logger);
      } else {
         this.readTimeoutInMillis = 0L;
         this.user = this.clientURI.getUsername();
         this.authDatabase = this.clientURI.getDatabase() != null && !this.clientURI.getDatabase().isEmpty() ? this.clientURI.getDatabase() : "admin";
         this.clientOptions = MongoClientOptionsHelper.newMongoClientOptions(this.clientURI);
         this.addressClientMap = CacheBuilder.newBuilder().expireAfterAccess(24L, TimeUnit.HOURS).removalListener(new MongoConnectionManager.AddressCloser()).build();
      }
   }

   private static StringBuilder appendHosts(StringBuilder sb, Iterable<Host> hostList, char delimiter) {
      Iterator iterator = hostList.iterator();

      while(iterator.hasNext()) {
         Host h = (Host)iterator.next();
         sb.append((String)Preconditions.checkNotNull(h.hostname, "hostname missing")).append(":").append(Preconditions.checkNotNull(h.port, "port missing"));
         if (iterator.hasNext()) {
            sb.append(delimiter);
         }
      }

      return sb;
   }

   static String getHosts(Iterable<Host> hostList, char delimiter) {
      return appendHosts(new StringBuilder(), hostList, delimiter).toString();
   }

   public MongoVersion connect() {
      return this.validateConnection();
   }

   private static String urlEncode(String fragment) {
      try {
         return URLEncoder.encode(fragment, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException var2) {
         throw new AssertionError("Expecting UTF_8 to be a supported charset", var2);
      }
   }

   public MongoConnection getFirstConnection() {
      String firstHost = (String)this.clientURI.getHosts().iterator().next();
      String[] firstHostAndPort = firstHost.split(":");
      ServerAddress address = firstHostAndPort.length == 2 ? new ServerAddress(firstHostAndPort[0], Integer.parseInt(firstHostAndPort[1])) : new ServerAddress(firstHost);
      return this.getClient(ImmutableList.of(address), this.authTimeoutInMillis);
   }

   public MongoTopology getTopology(boolean canDbStats, String dbStatsDatabase) {
      List<String> hosts = this.clientURI.getHosts();
      List<ServerAddress> addresses = Lists.newArrayList();
      Iterator var5 = hosts.iterator();

      while(var5.hasNext()) {
         String host = (String)var5.next();
         addresses.add(new ServerAddress(host));
      }

      MongoConnection connect = this.getClient(addresses, this.authTimeoutInMillis);
      return MongoTopology.getClusterTopology(this, connect, canDbStats, dbStatsDatabase, this.authDatabase, this.canReadOnlyOnSecondary);
   }

   @VisibleForTesting
   MongoClientURI getClientURI() {
      return this.clientURI;
   }

   @VisibleForTesting
   boolean getCanReadOnlyOnSecondary() {
      return this.canReadOnlyOnSecondary;
   }

   @VisibleForTesting
   long getAuthTimeoutInMillis() {
      return this.authTimeoutInMillis;
   }

   private MongoConnection getHost(long timeout) {
      List<String> hosts = this.clientURI.getHosts();
      List<ServerAddress> addresses = Lists.newArrayList();
      Iterator var5 = hosts.iterator();

      while(var5.hasNext()) {
         String host = (String)var5.next();
         addresses.add(new ServerAddress(host));
      }

      MongoConnection connect = this.getClient(addresses, timeout);
      return connect;
   }

   public MongoConnection getMetadataClient() {
      return this.getHost(this.authTimeoutInMillis);
   }

   public MongoConnection getMetadataClient(ServerAddress host) {
      return this.getClient(Collections.singletonList(host), this.authTimeoutInMillis);
   }

   public MongoConnection getReadClient() {
      return this.getHost(this.readTimeoutInMillis);
   }

   public MongoConnection getReadClients(List<ServerAddress> hosts) {
      return this.getClient(hosts, this.readTimeoutInMillis);
   }

   private synchronized MongoConnection getClient(List<ServerAddress> addresses, long serverSelectionTimeout) {
      if (addresses.isEmpty()) {
         throw UserException.dataReadError().message("Failure while attempting to connect to server without addresses.", new Object[0]).build(logger);
      } else {
         ServerAddress serverAddress = (ServerAddress)addresses.get(0);
         MongoCredential credential = this.clientURI.getCredentials();
         String userName = credential == null ? null : credential.getUserName();
         MongoConnectionKey key = new MongoConnectionKey(serverAddress, userName);
         List<MongoCredential> credentials = credential != null ? Collections.singletonList(credential) : Collections.emptyList();
         MongoClient client = (MongoClient)this.addressClientMap.getIfPresent(key);
         if (client == null) {
            MongoClientOptions localClientOptions = this.clientOptions;
            if (serverSelectionTimeout > 0L) {
               localClientOptions = MongoClientOptions.builder(localClientOptions).serverSelectionTimeout((int)serverSelectionTimeout).build();
            }

            if (addresses.size() > 1) {
               client = new MongoClient(addresses, credentials, localClientOptions);
            } else {
               client = new MongoClient((ServerAddress)addresses.get(0), credentials, localClientOptions);
            }

            this.addressClientMap.put(key, client);
            logger.info("Created connection to {}.", key.toString());
            logger.info("Number of open connections {}.", this.addressClientMap.size());
            logger.info("MongoClientOptions:\n" + localClientOptions);
            logger.info("MongoCredential:" + credential);
         }

         return new MongoConnection(client, this.authDatabase, this.user);
      }
   }

   public MongoVersion validateConnection() {
      MongoConnection client = this.getMetadataClient();
      client.authenticate();
      MongoCollections fetchResult = client.getCollections();
      if (fetchResult != null && !fetchResult.isEmpty()) {
         MongoTopology topology = this.getTopology(fetchResult.canDBStats(), fetchResult.getDatabaseToDBStats());
         if (this.authDatabase != null) {
            logger.debug("Connected as user [{}], using authenticationDatabase [{}]. Found {} collections. {}", new Object[]{this.user, this.authDatabase, fetchResult.getNumDatabases(), topology});
         } else {
            logger.debug("Connected as anonymous user. Found {} collections. {}", fetchResult.getNumDatabases(), topology);
         }

         return topology.getClusterVersion();
      } else if (this.authDatabase == null) {
         throw StoragePluginUtils.message(UserException.dataReadError(), this.pluginName, "Connection to Mongo failed. System either had no user collections or user %s was unable to access them.", new Object[]{this.user}).build(logger);
      } else {
         throw StoragePluginUtils.message(UserException.dataReadError(), this.pluginName, "Connection to Mongo failed. No collections were visible.", new Object[0]).build(logger);
      }
   }

   public void close() throws Exception {
      this.addressClientMap.invalidateAll();
   }

   private class AddressCloser implements RemovalListener<MongoConnectionKey, MongoClient> {
      private AddressCloser() {
      }

      public synchronized void onRemoval(RemovalNotification<MongoConnectionKey, MongoClient> removal) {
         ((MongoClient)removal.getValue()).close();
         MongoConnectionManager.logger.debug("Closed connection to {}.", ((MongoConnectionKey)removal.getKey()).toString());
      }

      // $FF: synthetic method
      AddressCloser(Object x1) {
         this();
      }
   }
}
