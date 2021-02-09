package com.dremio.plugins.mongo;

import com.dremio.plugins.Version;
import org.bson.Document;

public interface MongoConstants {
   String ID = "_id";
   String NS = "ns";
   String SHARD = "shard";
   String HOSTS = "hosts";
   String CHUNKS = "chunks";
   String KEY = "key";
   String HASHED = "hashed";
   String CONFIG_DB = "config";
   String LOCAL_DB = "local";
   String MIN = "min";
   String MAX = "max";
   String PRIMARY = "primary";
   String DATABASES = "databases";
   String ADMIN_DB = "admin";
   String OK = "ok";
   String SYSCOLLECTION_PREFIX = "system.";
   Version VERSION_0_0 = new Version(0, 0, 0);
   Version VERSION_3_2 = new Version(3, 2, 0);
   Document PING_REQ = new Document("ping", 1);
}
