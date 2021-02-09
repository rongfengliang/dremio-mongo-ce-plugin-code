package com.dremio.plugins.mongo.metadata;

import com.dremio.plugins.Version;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.mongodb.client.MongoDatabase;
import java.util.Arrays;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoVersion extends Version {
   private static final Logger logger = LoggerFactory.getLogger(MongoVersion.class);
   public static final MongoVersion MIN_MONGO_VERSION = new MongoVersion(1, 0, 0);
   public static final MongoVersion MAX_MONGO_VERSION = new MongoVersion(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
   public static final MongoVersion MIN_VERSION_TO_ENABLE_NEW_FEATURES = new MongoVersion(3, 2, 0);
   public static final Version NEW_FEATURE_CUTOFF_VERSION = new Version(3, 2, 0);
   private final int compatibilityMajor;
   private final int compatibilityMinor;

   public MongoVersion(int major, int minor, int patch) {
      super(major, minor, patch);
      this.compatibilityMajor = major;
      this.compatibilityMinor = minor;
   }

   public MongoVersion(int major, int minor, int patch, int compatibilityMajor, int compatibilityMinor) {
      super(major, minor, patch);
      this.compatibilityMajor = compatibilityMajor;
      this.compatibilityMinor = compatibilityMinor;
   }

   public boolean enableNewFeatures() {
      return this.compareTo(NEW_FEATURE_CUTOFF_VERSION) >= 0;
   }

   static MongoVersion getVersionForConnection(MongoDatabase database) {
      Version version;
      Document compatibilityResult;
      try {
         compatibilityResult = database.runCommand(new BsonDocument("buildInfo", new BsonBoolean(true)));
         version = Version.parse(compatibilityResult.getString("version"));
      } catch (Exception var6) {
         logger.warn("Could not get the mongo version from the server. Defaulting to version: {}", MAX_MONGO_VERSION, var6);
         return MAX_MONGO_VERSION;
      }

      if (version.getMajor() == 3 && version.getMinor() >= 4 || version.getMajor() >= 4) {
         try {
            compatibilityResult = database.runCommand(new BsonDocument(Arrays.asList(new BsonElement("getParameter", new BsonBoolean(true)), new BsonElement("featureCompatibilityVersion", new BsonBoolean(true)))));
         } catch (Exception var5) {
            logger.warn("Could not get the mongo compatibility version from the server. Defaulting to detected version: {}.{}", new Object[]{version.getMajor(), version.getMinor(), var5});
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), version.getMajor(), version.getMinor());
         }

         if (version.getMajor() == 3 && version.getMinor() == 4) {
            Version compatibility = Version.parse(compatibilityResult.getString("featureCompatibilityVersion"));
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), compatibility.getMajor(), compatibility.getMinor());
         }

         if (version.getMajor() == 3 && version.getMinor() > 4 || version.getMajor() >= 4) {
            Document featureCompatibilityVersion = (Document)compatibilityResult.get("featureCompatibilityVersion");
            Version compatibility = Version.parse(featureCompatibilityVersion.getString("version"));
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), compatibility.getMajor(), compatibility.getMinor());
         }
      }

      return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch());
   }

   public Message getVersionInfo() {
      String message = String.format("MongoDB version %s.", this);
      return new Message(MessageLevel.INFO, message);
   }

   public Message getVersionWarning() {
      String message = String.format("Detected MongoDB version %s. Full query pushdown in Dremio requires version %s", this, NEW_FEATURE_CUTOFF_VERSION);
      return new Message(MessageLevel.WARN, message);
   }

   public int getCompatibilityMajor() {
      return this.compatibilityMajor;
   }

   public int getCompatibilityMinor() {
      return this.compatibilityMinor;
   }
}
