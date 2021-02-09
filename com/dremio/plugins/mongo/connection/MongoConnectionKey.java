package com.dremio.plugins.mongo.connection;

import com.mongodb.ServerAddress;

public class MongoConnectionKey {
   private final ServerAddress address;
   private final String user;

   public MongoConnectionKey(ServerAddress address, String user) {
      this.address = address;
      this.user = user;
   }

   public int hashCode() {
      int prime = true;
      int result = 1;
      int result = 31 * result + (this.address == null ? 0 : this.address.hashCode());
      result = 31 * result + (this.user == null ? 0 : this.user.hashCode());
      return result;
   }

   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      } else if (obj == null) {
         return false;
      } else if (this.getClass() != obj.getClass()) {
         return false;
      } else {
         MongoConnectionKey other = (MongoConnectionKey)obj;
         if (this.address == null) {
            if (other.address != null) {
               return false;
            }
         } else if (!this.address.equals(other.address)) {
            return false;
         }

         if (this.user == null) {
            if (other.user != null) {
               return false;
            }
         } else if (!this.user.equals(other.user)) {
            return false;
         }

         return true;
      }
   }

   public String toString() {
      return "[address=" + this.address.toString() + ", user=" + this.user + "]";
   }
}
