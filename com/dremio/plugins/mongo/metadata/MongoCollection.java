package com.dremio.plugins.mongo.metadata;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class MongoCollection {
   private final String database;
   private final String collection;

   public MongoCollection(String database, String collection) {
      this.database = database;
      this.collection = collection;
   }

   public String getDatabase() {
      return this.database;
   }

   public String getCollection() {
      return this.collection;
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.database, this.collection});
   }

   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      } else if (obj == null) {
         return false;
      } else if (this.getClass() != obj.getClass()) {
         return false;
      } else {
         MongoCollection other = (MongoCollection)obj;
         return Objects.equals(this.database, other.database) && Objects.equals(this.collection, other.collection);
      }
   }

   public boolean equalsIgnoreCase(MongoCollection other) {
      return this.database.equalsIgnoreCase(other.database) && this.collection.equalsIgnoreCase(other.collection);
   }

   public String toName() {
      return this.database + '.' + this.collection;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("database", this.database).add("collection", this.collection).toString();
   }
}
