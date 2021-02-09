package com.dremio.plugins.mongo.connection;

import com.dremio.common.exceptions.UserException;
import com.dremio.plugins.mongo.MongoConstants;
import com.dremio.plugins.mongo.MongoDocumentHelper;
import com.dremio.plugins.mongo.metadata.MongoCollections;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoServerException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoConnection {
   private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);
   private final MongoClient client;
   private final String authDatabase;
   private final String user;

   public MongoConnection(MongoClient client, String authDatabase, String user) {
      this.client = client;
      this.authDatabase = authDatabase;
      this.user = user;
   }

   public MongoCollections getCollections() {
      return MongoCollections.fetchDatabaseAndCollectionNames(this.client, this.user, this.authDatabase);
   }

   public ServerAddress getAddress() {
      return this.client.getAddress();
   }

   public MongoClient getClient() {
      return this.client;
   }

   public MongoDatabase getDatabase(String name) {
      return this.client.getDatabase(name);
   }

   public void authenticate() {
      if (this.user != null && !this.user.isEmpty()) {
         MongoDatabase db = this.client.getDatabase(this.authDatabase);
         if (db == null) {
            throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[]{this.authDatabase}).build(logger);
         } else {
            String errorMessage = "Could not authenticate user `" + this.user + "` for database `" + this.authDatabase + "`";

            try {
               Document result = MongoDocumentHelper.runMongoCommand(db, MongoConstants.PING_REQ);
               if (!MongoDocumentHelper.checkResultOkay(result, errorMessage)) {
                  throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[]{this.authDatabase}).build(logger);
               }
            } catch (MongoServerException | MongoClientException var4) {
               throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[]{this.authDatabase}).build(logger);
            }
         }
      }
   }
}
