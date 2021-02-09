package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.plugins.mongo.planning.rels.MongoRel;
import org.apache.calcite.rel.RelNode;

public class MongoImplementor {
   private final StoragePluginId pluginId;
   private boolean needsLimitZero = false;
   private boolean hasSample = false;
   private long limitSize = Long.MAX_VALUE;
   private boolean hasLimit = false;

   public MongoImplementor(StoragePluginId pluginId) {
      this.pluginId = pluginId;
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public MongoScanSpec visitChild(int i, RelNode e) {
      return ((MongoRel)e).implement(this);
   }

   public void markAsNeedsLimitZero() {
      this.needsLimitZero = true;
   }

   public boolean needsLimitZero() {
      return this.needsLimitZero;
   }

   public void setHasSample() {
      this.hasSample = true;
   }

   public boolean hasSample() {
      return this.hasSample;
   }

   public void setHasLimit(long limitSize) {
      this.hasLimit = true;
      if (this.limitSize > limitSize) {
         this.limitSize = limitSize;
      }

   }

   public boolean hasLimit() {
      return this.hasLimit;
   }

   public long getLimitSize() {
      return this.limitSize;
   }
}
