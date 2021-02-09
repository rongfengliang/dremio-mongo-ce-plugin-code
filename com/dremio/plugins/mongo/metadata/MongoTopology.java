package com.dremio.plugins.mongo.metadata;

import com.dremio.common.exceptions.UserException;
import com.dremio.plugins.mongo.MongoConstants;
import com.dremio.plugins.mongo.MongoDocumentHelper;
import com.dremio.plugins.mongo.connection.MongoConnection;
import com.dremio.plugins.mongo.connection.MongoConnectionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.mongodb.MongoClientException;
import com.mongodb.MongoServerException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoTopology implements Iterable<MongoTopology.ReplicaSet> {
   private static final Logger logger = LoggerFactory.getLogger(MongoTopology.class);
   private final ImmutableList<MongoTopology.ReplicaSet> replicaSets;
   private final boolean isSharded;
   private final boolean canGetChunks;
   private final boolean canConnectToMongods;
   private final MongoVersion minimumClusterVersion;

   @VisibleForTesting
   public MongoTopology(List<MongoTopology.ReplicaSet> replicaSets, boolean isSharded, boolean canGetChunks, boolean canConnectToMongods, MongoVersion minimumClusterVersion) {
      this.isSharded = isSharded;
      this.canGetChunks = canGetChunks;
      this.canConnectToMongods = canConnectToMongods;
      this.replicaSets = ImmutableList.copyOf(replicaSets);
      this.minimumClusterVersion = minimumClusterVersion;
   }

   public boolean isSharded() {
      return this.isSharded;
   }

   public boolean canGetChunks() {
      return this.canGetChunks;
   }

   public boolean canConnectToMongods() {
      return this.canConnectToMongods;
   }

   public MongoVersion getClusterVersion() {
      return this.minimumClusterVersion;
   }

   public Iterator<MongoTopology.ReplicaSet> iterator() {
      return this.replicaSets.iterator();
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("MongoTopology\n");
      sb.append('\n');
      sb.append("\t-Is Sharded Cluster: ");
      sb.append(this.isSharded);
      sb.append("\n\t-Can Get Chunks (applicable only to sharded cluster)?: ");
      sb.append(this.canGetChunks);
      sb.append("\n\t-Can Connect to MongoDs (applicable only to sharded cluster)?: ");
      sb.append(this.canConnectToMongods);
      sb.append("\n\n\tReplicas:\n");
      UnmodifiableIterator var2 = this.replicaSets.iterator();

      while(var2.hasNext()) {
         MongoTopology.ReplicaSet r = (MongoTopology.ReplicaSet)var2.next();
         sb.append("\t\t");
         sb.append(r.getName());
         sb.append(": ");
         Iterator var4 = r.getNodes().iterator();

         while(var4.hasNext()) {
            MongoTopology.MongoServer server = (MongoTopology.MongoServer)var4.next();
            sb.append(server.address.toString());
            sb.append(" (");
            sb.append(server.role.name().toLowerCase());
            sb.append("), ");
         }

         sb.append("\n");
      }

      return sb.toString();
   }

   public static MongoTopology getClusterTopology(MongoConnectionManager manager, MongoConnection client, boolean canDbStats, String dbStatsDatabase, String authDatabase, boolean readOnlySecondary) {
      MongoDatabase db = client.getDatabase(authDatabase);
      Document isMasterMsg = MongoDocumentHelper.runMongoCommand(db, new Document("isMaster", 1));
      if (!MongoDocumentHelper.checkResultOkay(isMasterMsg, "Failed to run mongo command, isMaster")) {
         throw UserException.dataReadError().message("Unable to retrieve mongo cluster information", new Object[0]).build(logger);
      } else {
         boolean isMongos = "isdbgrid".equalsIgnoreCase(isMasterMsg.getString("msg"));
         if (!isMongos) {
            logger.debug("Detected that we are working with mongod according to isMaster response {}", isMasterMsg);
            MongoTopology.ReplicaSet replica = getReplicaSet("singlenode", client.getAddress(), isMasterMsg, readOnlySecondary);
            MongoVersion minVersion = getMinVersion(manager, authDatabase, Lists.newArrayList(new MongoTopology.ReplicaSet[]{replica}));
            return new MongoTopology(Collections.singletonList(replica), isMongos, false, false, minVersion);
         } else {
            logger.debug("Detected that we are working with mongos according to isMaster response {}", isMasterMsg);
            if (!canDbStats) {
               logger.warn("Detected that we are working with mongos, but cannot dbStats any database.  Cannot get mongo shard information");
               return new MongoTopology(Lists.newArrayList(), isMongos, false, false, MongoVersion.getVersionForConnection(db));
            } else {
               boolean singleThreaded = false;
               MongoDatabase dbForStats = client.getDatabase(dbStatsDatabase);
               List<MongoTopology.Shard> shards = getShards(dbForStats);
               if (shards.isEmpty()) {
                  logger.warn("Unable to retrieve shards from mongo server, connecting to mongos only");
                  singleThreaded = true;
               }

               List<MongoTopology.ReplicaSet> replicas = new ArrayList();
               Iterator var13 = shards.iterator();

               while(var13.hasNext()) {
                  MongoTopology.Shard shard = (MongoTopology.Shard)var13.next();
                  ServerAddress randomNode = (ServerAddress)shard.addresses.get(0);
                  MongoConnection randomClient = manager.getMetadataClient(randomNode);
                  MongoDatabase randomDB = randomClient.getDatabase(authDatabase);
                  if (!canConnectTo(randomDB, randomNode)) {
                     singleThreaded = true;
                     replicas.clear();
                     break;
                  }

                  Document randomIsMaster = MongoDocumentHelper.runMongoCommand(randomDB, new Document("isMaster", 1));
                  if (!MongoDocumentHelper.checkResultOkay(randomIsMaster, "Failed to run mongo command, isMaster")) {
                     throw UserException.dataReadError().message("Unable to retrieve mongo cluster information", new Object[0]).build(logger);
                  }

                  replicas.add(getReplicaSet(shard.name, randomNode, randomIsMaster, readOnlySecondary));
               }

               if (singleThreaded) {
                  replicas.add(getReplicaSet("mongosnode", client.getAddress(), isMasterMsg, readOnlySecondary));
               }

               MongoVersion version = getMinVersion(manager, authDatabase, replicas);
               return new MongoTopology(replicas, isMongos, canGetChunks(client), !singleThreaded, version);
            }
         }
      }
   }

   private static MongoVersion getMinVersion(MongoConnectionManager clientProvider, String authDb, List<MongoTopology.ReplicaSet> replicaSets) {
      MongoVersion minVersionFoundInCluster = new MongoVersion(Integer.MAX_VALUE, 0, 0);
      Set<String> hostsCheckedForVersion = new HashSet();
      Iterator var5 = replicaSets.iterator();

      while(var5.hasNext()) {
         MongoTopology.ReplicaSet replSet = (MongoTopology.ReplicaSet)var5.next();
         Iterator var7 = replSet.getNodes().iterator();

         while(var7.hasNext()) {
            MongoTopology.MongoServer server = (MongoTopology.MongoServer)var7.next();
            String addr = server.getAddress().toString();
            if (!hostsCheckedForVersion.contains(addr)) {
               MongoDatabase database = clientProvider.getMetadataClient(new ServerAddress(addr)).getDatabase(authDb);
               MongoVersion version = MongoVersion.getVersionForConnection(database);
               if (version.compareTo(minVersionFoundInCluster) < 0) {
                  minVersionFoundInCluster = version;
               }
            }
         }
      }

      return minVersionFoundInCluster;
   }

   private static boolean canConnectTo(MongoDatabase db, ServerAddress hostAddr) {
      String var2 = "User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode";

      try {
         Document result = MongoDocumentHelper.runMongoCommand(db, MongoConstants.PING_REQ);
         if (MongoDocumentHelper.checkResultOkay(result, "Failed to ping mongo cluster")) {
            return true;
         }

         logger.warn("User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode", hostAddr);
      } catch (MongoServerException | MongoClientException var4) {
         logger.warn("User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode", hostAddr, var4);
      }

      return false;
   }

   private static boolean canGetChunks(MongoConnection client) {
      try {
         MongoDatabase configDB = client.getDatabase("config");
         com.mongodb.client.MongoCollection<Document> chunksCollection = configDB.getCollection("chunks");
         FindIterable<Document> chunkCursor = chunksCollection.find().batchSize(1).limit(1);
         if (chunkCursor.iterator().hasNext()) {
            chunkCursor.iterator().next();
            return true;
         } else {
            logger.info("Connected to unsharded mongo database, found zero chunks");
            return false;
         }
      } catch (Exception var4) {
         logger.warn("User does not have access to config.chunks, this will slow down performance.Entering single threaded mode : " + var4);
         return false;
      }
   }

   private static MongoTopology.ReplicaSet getReplicaSet(String name, ServerAddress host, Document isMasterMsg, boolean readOnlySecondary) {
      ArrayList<String> hosts = (ArrayList)isMasterMsg.get("hosts");
      if (hosts == null) {
         return new MongoTopology.ReplicaSet(name, Collections.singletonList(new MongoTopology.MongoServer(host, MongoTopology.ServerRole.PRIMARY)));
      } else {
         String primary = isMasterMsg.getString("primary");
         List<MongoTopology.MongoServer> nodes = new ArrayList(hosts.size());
         Iterator var7 = hosts.iterator();

         while(true) {
            String hostString;
            MongoTopology.ServerRole role;
            do {
               if (!var7.hasNext()) {
                  return new MongoTopology.ReplicaSet(name, nodes);
               }

               Object o = var7.next();
               hostString = (String)o;
               role = primary.equals(hostString) ? MongoTopology.ServerRole.PRIMARY : MongoTopology.ServerRole.SECONDARY;
            } while(role != MongoTopology.ServerRole.SECONDARY && readOnlySecondary);

            String[] hostStringParts = hostString.split(":");
            MongoTopology.MongoServer n = new MongoTopology.MongoServer(new ServerAddress(hostStringParts[0], Integer.parseInt(hostStringParts[1])), role);
            nodes.add(n);
         }
      }
   }

   private static MongoTopology.Shard getShard(String shardString) {
      String[] tagAndHost = StringUtils.split(shardString, '/');
      if (tagAndHost.length >= 1 && tagAndHost.length <= 2) {
         String[] hosts = tagAndHost.length > 1 ? StringUtils.split(tagAndHost[1], ',') : StringUtils.split(tagAndHost[0], ',');
         String tag = tagAndHost[0];
         List<ServerAddress> addresses = Lists.newArrayList();
         String[] var5 = hosts;
         int var6 = hosts.length;

         for(int var7 = 0; var7 < var6; ++var7) {
            String host = var5[var7];
            String[] hostAndPort = host.split(":");
            switch(hostAndPort.length) {
            case 1:
               addresses.add(new ServerAddress(hostAndPort[0]));
               break;
            case 2:
               try {
                  addresses.add(new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
               } catch (NumberFormatException var11) {
               }
               break;
            default:
               return null;
            }
         }

         if (addresses.isEmpty()) {
            return null;
         } else {
            return new MongoTopology.Shard(tag, addresses);
         }
      } else {
         return null;
      }
   }

   private static List<MongoTopology.Shard> getShards(MongoDatabase db) {
      Document dbStats = MongoDocumentHelper.runMongoCommand(db, new Document("dbStats", 1));
      MongoDocumentHelper.checkResultOkay(dbStats, "Mongo command, dbStats, returned error");
      Document raw = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(dbStats, "raw");
      if (raw == null) {
         logger.warn("Attempted to get shards from mongod. Shards will only be available when asking mongos.");
         return Collections.emptyList();
      } else {
         Preconditions.checkNotNull(raw, "Tried to get shard info on a non-sharded cluster.");
         List<MongoTopology.Shard> shards = new ArrayList();
         Iterator var4 = raw.keySet().iterator();

         while(var4.hasNext()) {
            String hostEntry = (String)var4.next();
            MongoTopology.Shard shard = getShard(hostEntry);
            if (shard == null) {
               logger.warn("Failure trying to get shard names from dbStats. String {} didn't match expected pattern of [name]/host:port,host:port...", shard);
               return Collections.emptyList();
            }

            shards.add(shard);
         }

         return shards;
      }
   }

   private static final class Shard {
      private final String name;
      private final List<ServerAddress> addresses;

      public Shard(String name, List<ServerAddress> addresses) {
         this.name = name;
         this.addresses = addresses;
      }
   }

   public static final class ReplicaSet {
      private final String name;
      private final List<MongoTopology.MongoServer> nodes;

      public ReplicaSet(String name, List<MongoTopology.MongoServer> nodes) {
         this.name = name;
         this.nodes = nodes;
      }

      public String getName() {
         return this.name;
      }

      public List<MongoTopology.MongoServer> getNodes() {
         return this.nodes;
      }

      public MongoChunk getAsChunk(int index) {
         List<ServerAddress> addresses = Lists.newArrayList();
         Iterator var3 = this.nodes.iterator();

         while(var3.hasNext()) {
            MongoTopology.MongoServer s = (MongoTopology.MongoServer)var3.next();
            addresses.add(s.address);
         }

         if (addresses.isEmpty()) {
            throw UserException.dataReadError().message("Unable to find valid nodes to read from. This typically happens when you configure Dremio to avoid reading from primary and no secondary is available.", new Object[0]).build(MongoTopology.logger);
         } else {
            return MongoChunk.newWithAddresses(addresses);
         }
      }
   }

   public static final class MongoServer {
      private final ServerAddress address;
      private final MongoTopology.ServerRole role;

      public MongoServer(ServerAddress address, MongoTopology.ServerRole role) {
         this.address = address;
         this.role = role;
      }

      public ServerAddress getAddress() {
         return this.address;
      }

      public MongoTopology.ServerRole getRole() {
         return this.role;
      }
   }

   public static enum ServerRole {
      PRIMARY,
      SECONDARY;
   }
}
