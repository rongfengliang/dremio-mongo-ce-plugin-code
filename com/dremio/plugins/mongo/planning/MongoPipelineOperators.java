package com.dremio.plugins.mongo.planning;

import com.google.common.collect.Sets;
import java.util.Set;

public enum MongoPipelineOperators {
   PROJECT("$project"),
   UNWIND("$unwind"),
   SORT("$sort"),
   MATCH("$match");

   public static final Set<MongoPipelineOperators> PROJECT_ONLY = Sets.newHashSet(new MongoPipelineOperators[]{PROJECT});
   public static final Set<MongoPipelineOperators> MATCH_ONLY = Sets.newHashSet(new MongoPipelineOperators[]{MATCH});
   public static final Set<MongoPipelineOperators> PROJECT_MATCH = Sets.newHashSet(new MongoPipelineOperators[]{PROJECT, MATCH});
   private final String mongoOperator;

   private MongoPipelineOperators(String mongoOperator) {
      this.mongoOperator = mongoOperator;
   }

   public String getOperator() {
      return this.mongoOperator;
   }
}
