package com.dremio.plugins.mongo.planning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class MongoSubScanSpecList {
   private List<MongoSubScanSpec> specs;

   @JsonCreator
   public MongoSubScanSpecList(@JsonProperty("specs") List<MongoSubScanSpec> specs) {
      this.specs = specs;
   }

   public List<MongoSubScanSpec> getSpecs() {
      return this.specs;
   }
}
