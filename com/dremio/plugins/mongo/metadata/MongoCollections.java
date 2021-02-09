package com.dremio.plugins.mongo.metadata;

import com.dremio.plugins.mongo.MongoDocumentHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList.Builder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoServerException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoCollections implements Iterable<MongoCollection> {
   private static final Logger logger = LoggerFactory.getLogger(MongoCollections.class);
   public static final String DB = "db";
   public static final String USER = "user";
   private final ImmutableList<MongoCollection> allCollections;
   private final boolean canDBStats;
   private final String databaseToDBStats;
   private final String authenticationDB;

   public MongoCollections(ImmutableList<MongoCollection> databasesAndCollections, boolean canDBStats, String databaseToDBStats, String authenticationDB) {
      if (databasesAndCollections == null) {
         this.allCollections = ImmutableList.of();
      } else {
         this.allCollections = databasesAndCollections;
      }

      this.canDBStats = canDBStats;
      this.databaseToDBStats = databaseToDBStats;
      this.authenticationDB = authenticationDB;

      assert !this.canDBStats || databaseToDBStats != null && !databaseToDBStats.isEmpty() : "Database to run dbStats command cannot be null";
   }

   public List<MongoCollection> getDatabasesAndCollections() {
      return this.allCollections;
   }

   public boolean canDBStats() {
      return this.canDBStats;
   }

   public String getDatabaseToDBStats() {
      return this.databaseToDBStats;
   }

   public String getAuthenticationDB() {
      return this.authenticationDB;
   }

   public boolean isEmpty() {
      return this.allCollections.isEmpty();
   }

   public int getNumDatabases() {
      return this.allCollections.size();
   }

   public ImmutableList<MongoCollection> getCollections() {
      return this.allCollections;
   }

   public static MongoCollections fetchDatabaseAndCollectionNames(MongoClient client, String user, String authenticationDatabase) {
      List<String> listAllNonSystemDatabases = null;
      List<String> listAllSystemDatabases = new ArrayList(2);
      String authDatabase;
      if (authenticationDatabase != null && !authenticationDatabase.isEmpty()) {
         authDatabase = authenticationDatabase;
      } else {
         authDatabase = "admin";
      }

      if (user != null && !user.isEmpty()) {
         MongoDatabase authDB = client.getDatabase(authDatabase);
         Document userDetails = new Document();
         userDetails.put("user", user);
         userDetails.put("db", authDatabase);
         Document getUserInfo = new Document();
         getUserInfo.put("usersInfo", userDetails);
         getUserInfo.put("showCredentials", false);
         getUserInfo.put("showPrivileges", true);
         Document result = MongoDocumentHelper.runMongoCommand(authDB, getUserInfo);
         if (!MongoDocumentHelper.checkResultOkay(result, "Could not get user information for `" + user + "`, authDB `" + authDatabase + "`")) {
            return new MongoCollections(ImmutableList.of(), false, (String)null, authDatabase);
         } else {
            List<Document> users = (List)MongoDocumentHelper.getObjectFromDocumentWithKey(result, "users");

            assert users.size() == 1 : "UserInfo should return a list of size 1 since we only provided one user";

            Document thisUser = (Document)users.get(0);
            List<Document> privileges = (List)MongoDocumentHelper.getObjectFromDocumentWithKey(thisUser, "inheritedPrivileges");
            boolean canListDatabases = canListDatabases(privileges);
            Builder<MongoCollection> collections = ImmutableList.builder();
            boolean canDBStats = false;
            String databaseToDBStats = null;
            Iterator var17 = privileges.iterator();

            while(true) {
               List actions;
               String resourceDB;
               String resourceCollection;
               boolean thisDBCanDBStats;
               boolean canFind;
               do {
                  do {
                     do {
                        do {
                           do {
                              Document resource;
                              do {
                                 if (!var17.hasNext()) {
                                    return new MongoCollections(collections.build(), canDBStats, databaseToDBStats, authDatabase);
                                 }

                                 Document privDoc = (Document)var17.next();
                                 resource = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "resource");
                                 actions = (List)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "actions");
                              } while(resource.get("db") == null);

                              resourceDB = resource.getString("db");
                              resourceCollection = resource.getString("collection");
                              thisDBCanDBStats = false;
                           } while(resourceCollection == null);
                        } while(resourceDB == null);
                     } while(isSystemCollection(resourceCollection));

                     if (resourceCollection.isEmpty() && !canDBStats) {
                        thisDBCanDBStats = actions.contains("dbStats");
                        if (thisDBCanDBStats) {
                           canDBStats = true;
                           databaseToDBStats = resourceDB;
                        }
                     }
                  } while(isSystemDatabase(resourceDB));

                  canFind = actions.contains("find");
               } while(!canFind);

               List<String> databases = null;
               if (resourceDB.isEmpty()) {
                  if (canListDatabases) {
                     if (listAllNonSystemDatabases == null) {
                        listAllNonSystemDatabases = getDatabasesFromListDatabases(client, listAllSystemDatabases);
                     }

                     databases = listAllNonSystemDatabases;
                  }
               } else {
                  databases = Lists.newArrayList(new String[]{resourceDB});
               }

               if (thisDBCanDBStats && (databaseToDBStats == null || databaseToDBStats.isEmpty())) {
                  databaseToDBStats = getDBStatsDatabaseName((List)databases, listAllSystemDatabases);
                  if (databaseToDBStats == null) {
                     canDBStats = false;
                     databaseToDBStats = null;
                  }
               }

               collections.addAll(getAllCollections(client, (List)databases, resourceCollection, actions));
            }
         }
      } else {
         List<String> databases = getDatabasesFromListDatabases(client, listAllSystemDatabases);
         ImmutableList<MongoCollection> collections = getAllCollections(client, databases, (String)null, (List)null);
         return new MongoCollections(collections, true, authDatabase, authDatabase);
      }
   }

   private static String getDBStatsDatabaseName(List<String> currentDatabases, List<String> sysDatabases) {
      if (!currentDatabases.isEmpty()) {
         return (String)currentDatabases.get(0);
      } else {
         return !sysDatabases.isEmpty() ? (String)sysDatabases.get(0) : null;
      }
   }

   private static boolean canListDatabases(List<Document> privileges) {
      Iterator var1 = privileges.iterator();

      Document resource;
      List actions;
      do {
         if (!var1.hasNext()) {
            return false;
         }

         Document privDoc = (Document)var1.next();
         resource = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "resource");
         actions = (List)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "actions");
      } while(resource.get("cluster") == null);

      return actions.contains("listDatabases");
   }

   private static List<String> getDatabasesFromListDatabases(MongoClient client, List<String> systemDBs) {
      List<String> dbNames = new ArrayList();
      MongoCursor databases = client.listDatabaseNames().iterator();

      while(databases.hasNext()) {
         String dbName = (String)databases.next();
         if (!isSystemDatabase(dbName)) {
            dbNames.add(dbName);
         } else {
            systemDBs.add(dbName);
         }
      }

      return dbNames;
   }

   private static ImmutableList<MongoCollection> getAllCollections(MongoClient client, List<String> databases, String resourceCollection, List<String> actions) {
      if (databases != null && !databases.isEmpty()) {
         Builder<MongoCollection> collections = ImmutableList.builder();
         Iterator var5 = databases.iterator();

         while(var5.hasNext()) {
            String dbToAdd = (String)var5.next();
            List<MongoCollection> collectionsToAdd = getCollections(client, dbToAdd, resourceCollection, actions);
            if (collectionsToAdd != null && collectionsToAdd.size() != 0) {
               collections.addAll(collectionsToAdd);
            }
         }

         return collections.build();
      } else {
         return ImmutableList.of();
      }
   }

   public Iterator<MongoCollection> iterator() {
      return this.allCollections.iterator();
   }

   private static List<MongoCollection> getCollections(MongoClient client, String db, String collection, List<String> actions) {
      if (collection != null && !collection.isEmpty()) {
         return isSystemCollection(collection) ? Lists.newArrayList() : Lists.newArrayList(new MongoCollection[]{new MongoCollection(db, collection)});
      } else {
         boolean canListCollection = actions == null || actions.contains("listCollections");
         MongoDatabase mongoDb = client.getDatabase(db);
         if (!canListCollection) {
            logger.warn("listCollection privilege was not explicitly given to user for database, " + db);
         }

         ArrayList collectionsToReturn = new ArrayList();

         try {
            MongoCursor iterator = mongoDb.listCollectionNames().iterator();

            while(iterator.hasNext()) {
               String colName = (String)iterator.next();
               if (!isSystemCollection(colName)) {
                  collectionsToReturn.add(new MongoCollection(db, colName));
               }
            }
         } catch (MongoServerException | MongoClientException var9) {
            logger.warn("listCollection failed for database, " + db);
         }

         return collectionsToReturn;
      }
   }

   private static boolean isSystemDatabase(String database) {
      return "local".equalsIgnoreCase(database) || "config".equalsIgnoreCase(database);
   }

   private static boolean isSystemCollection(String collection) {
      return collection != null && collection.startsWith("system.");
   }
}
