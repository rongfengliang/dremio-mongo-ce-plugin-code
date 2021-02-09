package com.dremio.plugins.mongo.planning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.Document;

public class MongoScanSpec {
   private final String dbName;
   private final String collectionName;
   private final MongoPipeline pipeline;

   @JsonCreator
   public MongoScanSpec(@JsonProperty("dbName") String dbName, @JsonProperty("collectionName") String collectionName, @JsonProperty("pipeline") MongoPipeline pipeline) {
      this.dbName = dbName;
      this.collectionName = collectionName;
      this.pipeline = pipeline;
   }

   public String getDbName() {
      return this.dbName;
   }

   public String getCollectionName() {
      return this.collectionName;
   }

   @JsonProperty("pipeline")
   public MongoPipeline getPipeline() {
      return this.pipeline;
   }

   @JsonIgnore
   public String getMongoQuery() {
      StringBuilder sb = new StringBuilder();
      sb.append("use ");
      sb.append(this.dbName);
      sb.append("; ");
      sb.append("db.");
      sb.append(this.collectionName);
      sb.append(".");
      sb.append(this.pipeline.toString());
      return sb.toString();
   }

   public String toString() {
      return "MongoScanSpec [dbName=" + this.dbName + ", collectionName=" + this.collectionName + ", pipeline=" + this.pipeline + "]";
   }

   public boolean equals(Object other) {
      if (!(other instanceof MongoScanSpec)) {
         return false;
      } else {
         MongoScanSpec that = (MongoScanSpec)other;
         boolean equals = Objects.equals(this.dbName, that.dbName) && Objects.equals(this.collectionName, that.collectionName) && Objects.equals(this.pipeline, that.pipeline);
         return equals;
      }
   }

   public MongoScanSpec plusPipeline(List<Document> operations, boolean needsCollation) {
      List<Document> pipes = new ArrayList(this.pipeline.getPipelines());
      pipes.addAll(operations);
      MongoPipeline newPipeline = MongoPipeline.createMongoPipeline(pipes, this.pipeline.needsCollation() || needsCollation);
      return new MongoScanSpec(this.dbName, this.collectionName, newPipeline);
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.dbName, this.collectionName, this.pipeline});
   }
}
