package com.dremio.plugins.mongo.planning;

import com.dremio.plugins.mongo.planning.rels.MongoRel;
import org.apache.calcite.plan.Convention.Impl;

public class MongoConvention extends Impl {
   public static final MongoConvention INSTANCE = new MongoConvention();

   private MongoConvention() {
      super("MONGO", MongoRel.class);
   }
}
