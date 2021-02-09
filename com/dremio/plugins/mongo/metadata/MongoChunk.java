package com.dremio.plugins.mongo.metadata;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.dremio.mongo.proto.MongoReaderProto;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.mongodb.ServerAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

public class MongoChunk {
   private static final Joiner SPLIT_KEY_JOINER = Joiner.on('-');
   private static final double SPLIT_DEFAULT_SIZE = 100000.0D;
   private final List<String> chunkLocList;
   private Document minFilters;
   private Document maxFilters;

   public MongoChunk(List<String> chunkLocList) {
      this.chunkLocList = chunkLocList;
   }

   public static MongoChunk newWithAddress(ServerAddress chunkLoc) {
      Preconditions.checkNotNull(chunkLoc);
      return new MongoChunk(Collections.singletonList(chunkLoc.toString()));
   }

   public static MongoChunk newWithAddresses(Collection<ServerAddress> chunkLocList) {
      List<String> list = new ArrayList();
      Iterator var2 = chunkLocList.iterator();

      while(var2.hasNext()) {
         ServerAddress a = (ServerAddress)var2.next();
         list.add(a.toString());
      }

      return new MongoChunk(list);
   }

   public PartitionChunk toSplit() {
      MongoReaderProto.MongoSplitXattr.Builder splitAttributes = MongoReaderProto.MongoSplitXattr.newBuilder();
      if (this.minFilters != null) {
         splitAttributes.setMinFilter(this.minFilters.toJson());
      }

      if (this.maxFilters != null) {
         splitAttributes.setMaxFilter(this.maxFilters.toJson());
      }

      if (this.chunkLocList != null && !this.chunkLocList.isEmpty()) {
         splitAttributes.addAllHosts(this.chunkLocList);
      }

      List<DatasetSplitAffinity> affinities = new ArrayList();
      Iterator var3 = this.chunkLocList.iterator();

      while(var3.hasNext()) {
         String host = (String)var3.next();
         affinities.add(DatasetSplitAffinity.of(host, 100000.0D));
      }

      MongoReaderProto.MongoSplitXattr extended = splitAttributes.build();
      DatasetSplit[] var10000 = new DatasetSplit[1];
      Objects.requireNonNull(extended);
      var10000[0] = DatasetSplit.of(affinities, 100000L, 100000L, extended::writeTo);
      return PartitionChunk.of(var10000);
   }

   public List<String> getChunkLocList() {
      return this.chunkLocList;
   }

   public void setMinFilters(Document minFilters) {
      this.minFilters = minFilters;
   }

   public Map<String, Object> getMinFilters() {
      return this.minFilters;
   }

   public void setMaxFilters(Document maxFilters) {
      this.maxFilters = maxFilters;
   }

   public Map<String, Object> getMaxFilters() {
      return this.maxFilters;
   }

   public String toString() {
      return "ChunkInfo [chunkLocList=" + this.chunkLocList + ", minFilters=" + this.minFilters + ", maxFilters=" + this.maxFilters + "]";
   }

   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      } else if (obj == null) {
         return false;
      } else if (this.getClass() != obj.getClass()) {
         return false;
      } else {
         MongoChunk that = (MongoChunk)obj;
         return Objects.equals(this.chunkLocList, that.chunkLocList) && Objects.equals(this.minFilters, that.minFilters) && Objects.equals(this.maxFilters, that.maxFilters);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.chunkLocList, this.minFilters, this.maxFilters});
   }

   @JsonIgnore
   public MongoChunk.MongoCompleteWork newCompleteWork(ExecutionNodeMap executionNodes) {
      List<EndpointAffinity> affinityList = new ArrayList();
      Iterator var3 = this.chunkLocList.iterator();

      while(var3.hasNext()) {
         String loc = (String)var3.next();
         NodeEndpoint ep = executionNodes.getEndpoint(loc);
         if (ep != null) {
            affinityList.add(new EndpointAffinity(ep, 1.0D));
         }
      }

      return new MongoChunk.MongoCompleteWork(affinityList);
   }

   public class MongoCompleteWork extends SimpleCompleteWork {
      public MongoCompleteWork(List<EndpointAffinity> affinity) {
         super(1000000L, affinity);
      }

      public MongoChunk getChunk() {
         return MongoChunk.this;
      }
   }
}
