package com.dremio.plugins.mongo.planning;

import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.mongo.proto.MongoReaderProto;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.bson.Document;

public class MongoSubScanSpec {
   private String dbName;
   private String collectionName;
   private List<String> hosts;
   private Document minFilters;
   private Document maxFilters;
   private MongoPipeline pipeline;

   @JsonCreator
   public MongoSubScanSpec(@JsonProperty("dbName") String dbName, @JsonProperty("collectionName") String collectionName, @JsonProperty("hosts") List<String> hosts, @JsonProperty("minFilters") String minFilters, @JsonProperty("maxFilters") String maxFilters, @JsonProperty("pipeline") MongoPipeline pipeline) {
      this.dbName = dbName;
      this.collectionName = collectionName;
      this.hosts = hosts;
      this.minFilters = minFilters != null && !minFilters.isEmpty() ? Document.parse(minFilters) : null;
      this.maxFilters = maxFilters != null && !maxFilters.isEmpty() ? Document.parse(maxFilters) : null;
      this.pipeline = pipeline;
   }

   public static MongoSubScanSpec of(MongoScanSpec scanSpec, SplitAndPartitionInfo split) {
      MongoReaderProto.MongoSplitXattr splitAttributes;
      try {
         splitAttributes = MongoReaderProto.MongoSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
      } catch (InvalidProtocolBufferException var4) {
         throw Throwables.propagate(var4);
      }

      MongoSubScanSpec subScanSpec = new MongoSubScanSpec(scanSpec.getDbName(), scanSpec.getCollectionName(), splitAttributes.getHostsList(), splitAttributes.getMinFilter(), splitAttributes.getMaxFilter(), scanSpec.getPipeline().copy());
      return subScanSpec;
   }

   MongoSubScanSpec() {
   }

   public String getDbName() {
      return this.dbName;
   }

   public MongoSubScanSpec setDbName(String dbName) {
      this.dbName = dbName;
      return this;
   }

   public String getCollectionName() {
      return this.collectionName;
   }

   public MongoSubScanSpec setCollectionName(String collectionName) {
      this.collectionName = collectionName;
      return this;
   }

   public List<String> getHosts() {
      return this.hosts;
   }

   public MongoSubScanSpec setHosts(List<String> hosts) {
      this.hosts = hosts;
      return this;
   }

   @JsonProperty
   public String getMinFilters() {
      return this.minFilters == null ? null : this.minFilters.toJson();
   }

   @JsonIgnore
   public Document getMinFiltersAsDocument() {
      return this.minFilters;
   }

   public MongoSubScanSpec setMinFilters(Document minFilters) {
      this.minFilters = minFilters;
      return this;
   }

   @JsonProperty
   public String getMaxFilters() {
      return this.maxFilters == null ? null : this.maxFilters.toJson();
   }

   @JsonIgnore
   public Document getMaxFiltersAsDocument() {
      return this.maxFilters;
   }

   public MongoSubScanSpec setMaxFilters(Document maxFilters) {
      this.maxFilters = maxFilters;
      return this;
   }

   public MongoPipeline getPipeline() {
      return this.pipeline;
   }

   public MongoSubScanSpec setPipeline(MongoPipeline pipeline) {
      this.pipeline = pipeline;
      return this;
   }

   public String toString() {
      return "MongoSubScanSpec [dbName=" + this.dbName + ", collectionName=" + this.collectionName + ", hosts=" + this.hosts + ", minFilters=" + this.minFilters + ", maxFilters=" + this.maxFilters + ", pipeline=" + this.pipeline + "]";
   }
}
