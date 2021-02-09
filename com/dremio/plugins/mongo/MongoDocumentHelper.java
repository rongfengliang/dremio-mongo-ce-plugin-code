package com.dremio.plugins.mongo;

import com.mongodb.MongoClientException;
import com.mongodb.MongoServerException;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDocumentHelper {
   private static final Logger logger = LoggerFactory.getLogger(MongoDocumentHelper.class);

   public static Document runMongoCommand(MongoDatabase db, Bson command) {
      Document result = null;

      try {
         result = db.runCommand(command);
         return result;
      } catch (MongoServerException | MongoClientException var4) {
         logger.error("Failed to run command `" + command.toString() + "`, exception : " + var4);
         return new Document("Exception", var4.toString());
      }
   }

   public static boolean checkResultOkay(Document result, String errorMsg) {
      if (result != null && result.containsKey("ok") && Double.valueOf(1.0D).equals(result.getDouble("ok"))) {
         return true;
      } else {
         logger.error("Mongo command returned with invalid return code (not OK), " + errorMsg + " : " + (result == null ? "null" : result.toJson().toString()));
         return false;
      }
   }

   public static Object getObjectFromDocumentWithKey(Document result, String key) {
      if (result != null && !result.isEmpty()) {
         if (key != null && key.length() != 0) {
            Object subResult = result.get(key);
            if (subResult == null) {
               logger.error("Cannot search for subdocument with key, " + key + " in document " + result.toJson().toString());
            }

            return subResult;
         } else {
            logger.error("Key is empty, cannot search for subdocument with empty key");
            return null;
         }
      } else {
         logger.error("Result document is empty, cannot search for subdocument with key, " + key);
         return null;
      }
   }
}
