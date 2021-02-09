package com.dremio.plugins.mongo.planning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoPipeline {
   private static final Logger logger = LoggerFactory.getLogger(MongoPipeline.class);
   private final Document match;
   private final Document project;
   private final boolean needsCollation;

   @JsonCreator
   public MongoPipeline(@JsonProperty("match") String match, @JsonProperty("project") String project, @JsonProperty("needsCollation") boolean needsCollation) {
      this(match == null ? null : Document.parse(match), project == null ? null : Document.parse(project), needsCollation);
   }

   public MongoPipeline(Document match, Document project, boolean needsCollation) {
      this.match = match;
      this.project = project;
      this.needsCollation = needsCollation;
   }

   public static MongoPipeline createMongoPipeline(List<Document> pipelinesInput, boolean needsCollation) {
      if (pipelinesInput != null && !pipelinesInput.isEmpty()) {
         Document projFromDoc11;
         Document matchFromDoc12;
         Document projFromDoc1;
         Document matchFromDoc21;
         if (pipelinesInput.size() == 1) {
            projFromDoc11 = (Document)pipelinesInput.get(0);
            matchFromDoc12 = getTrivialProject(projFromDoc11);
            projFromDoc1 = getMatch(projFromDoc11);
            if (projFromDoc1 != null) {
               return new MongoPipeline(projFromDoc1, (Document)null, needsCollation);
            }

            if (matchFromDoc12 != null) {
               matchFromDoc21 = matchFromDoc12.isEmpty() ? null : matchFromDoc12;
               return new MongoPipeline((Document)null, matchFromDoc21, needsCollation);
            }
         } else {
            Document matchFromDoc1;
            Document projFromDoc2;
            if (pipelinesInput.size() == 2) {
               projFromDoc11 = (Document)pipelinesInput.get(0);
               matchFromDoc12 = (Document)pipelinesInput.get(1);
               if (projFromDoc11 != null && matchFromDoc12 != null) {
                  projFromDoc1 = getTrivialProject(projFromDoc11);
                  matchFromDoc21 = getMatch(matchFromDoc12);
                  matchFromDoc1 = getMatch(projFromDoc11);
                  projFromDoc2 = getTrivialProject(matchFromDoc12);
                  if (projFromDoc1 != null && matchFromDoc21 != null) {
                     return new MongoPipeline(matchFromDoc21, projFromDoc1, needsCollation);
                  }

                  if (matchFromDoc1 != null && projFromDoc2 != null) {
                     return new MongoPipeline(matchFromDoc1, projFromDoc2, needsCollation);
                  }

                  if (projFromDoc1 != null && projFromDoc2 != null) {
                     return new MongoPipeline((Document)null, projFromDoc2, needsCollation);
                  }
               }
            } else if (pipelinesInput.size() == 3) {
               projFromDoc11 = getTrivialProject((Document)pipelinesInput.get(0));
               matchFromDoc12 = getMatch((Document)pipelinesInput.get(1));
               projFromDoc1 = getTrivialProject((Document)pipelinesInput.get(2));
               matchFromDoc21 = getMatch((Document)pipelinesInput.get(0));
               matchFromDoc1 = getTrivialProject((Document)pipelinesInput.get(1));
               projFromDoc2 = getTrivialProject((Document)pipelinesInput.get(2));
               if (projFromDoc11 != null && matchFromDoc12 != null && projFromDoc1 != null && isTrivialProject(projFromDoc11) && isTrivialProject(projFromDoc1)) {
                  return new MongoPipeline(matchFromDoc12, projFromDoc1, needsCollation);
               }

               if (matchFromDoc21 != null && matchFromDoc1 != null && projFromDoc2 != null) {
                  return new MongoPipeline(matchFromDoc21, projFromDoc2, needsCollation);
               }
            }
         }

         throw new IllegalStateException(String.format("Mongo aggregation framework support has been removed. Number of pipelines: %d.", pipelinesInput.size()));
      } else {
         return new MongoPipeline((Document)null, (Document)null, needsCollation);
      }
   }

   @JsonProperty
   public boolean needsCollation() {
      return this.needsCollation;
   }

   @JsonProperty
   public String getProject() {
      return this.project == null ? null : this.project.toJson();
   }

   @JsonIgnore
   public Document getProjectAsDocument() {
      return this.project;
   }

   @JsonProperty
   public String getMatch() {
      return this.match == null ? null : this.match.toJson();
   }

   @JsonIgnore
   public Document getMatchAsDocument() {
      return this.match;
   }

   @JsonIgnore
   public List<Document> getPipelines() {
      List<Document> pipelineToReturn = Lists.newArrayListWithCapacity(2);
      if (this.match != null) {
         pipelineToReturn.add(new Document(MongoPipelineOperators.MATCH.getOperator(), this.match));
      }

      if (this.project != null) {
         pipelineToReturn.add(new Document(MongoPipelineOperators.PROJECT.getOperator(), this.project));
      }

      return pipelineToReturn;
   }

   @JsonIgnore
   public MongoPipeline applyMinMaxFilter(Document minFilters, Document maxFilters) {
      Map<String, List<Document>> rangeFilter = MongoFilterMerger.mergeFilters(minFilters, maxFilters);
      if (rangeFilter != null && rangeFilter.size() != 0) {
         Document newMatch = buildFilters(this.match, rangeFilter);
         return new MongoPipeline(newMatch, this.getProjectAsDocument(), this.needsCollation());
      } else {
         return this;
      }
   }

   @JsonIgnore
   public boolean hasProject() {
      return this.getPipelines().stream().anyMatch((p) -> {
         return p.get(MongoPipelineOperators.PROJECT.getOperator()) != null;
      });
   }

   @JsonIgnore
   public MongoCursor<RawBsonDocument> getCursor(MongoCollection<RawBsonDocument> collection, int targetRecordCount) {
      logger.debug("Filters Applied : " + this.match);
      logger.debug("Fields Selected :" + this.project);
      FindIterable cursor;
      if (this.match == null) {
         cursor = collection.find();
      } else {
         cursor = collection.find(this.match);
      }

      if (this.project != null) {
         cursor = cursor.projection(this.project);
      }

      if (this.needsCollation) {
         cursor = cursor.collation(Collation.builder().locale("en_US").numericOrdering(true).build());
      }

      return cursor.batchSize(targetRecordCount).iterator();
   }

   public static Document getTrivialProject(Document pipelineEntry) {
      Document proj = (Document)pipelineEntry.get(MongoPipelineOperators.PROJECT.getOperator());
      return proj != null && isTrivialProject(proj) ? proj : null;
   }

   private static Document getMatch(Document pipelineEntry) {
      return (Document)pipelineEntry.get(MongoPipelineOperators.MATCH.getOperator());
   }

   @JsonIgnore
   public boolean isOnlyTrivialProjectOrFilter() {
      return !this.hasProject() || isTrivialProject(this.project);
   }

   @JsonIgnore
   public boolean isSimpleScan() {
      return !this.hasProject();
   }

   public boolean needsCoercion() {
      Iterator var1 = this.getPipelines().iterator();

      Document projectDoc;
      do {
         if (!var1.hasNext()) {
            return false;
         }

         Document pipe = (Document)var1.next();
         if (pipe.get(MongoPipelineOperators.UNWIND.getOperator()) != null) {
            return true;
         }

         projectDoc = (Document)pipe.get(MongoPipelineOperators.PROJECT.getOperator());
      } while(projectDoc == null || isTrivialProject(projectDoc));

      return true;
   }

   public static boolean isTrivialProject(Document projectDoc) {
      Iterator var1 = projectDoc.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<String, Object> entry = (Entry)var1.next();
         Object valueObj = entry.getValue();
         if (valueObj instanceof Integer) {
            if ((Integer)valueObj != 1) {
               return false;
            }
         } else {
            if (!(valueObj instanceof Boolean)) {
               return false;
            }

            if (!(Boolean)valueObj) {
               return false;
            }
         }
      }

      return true;
   }

   private static Document buildFilters(Document pushdownFilters, Map<String, List<Document>> mergedFilters) {
      Document toReturn = null;
      List<Document> listToAnd = new ArrayList();
      Iterator var4 = mergedFilters.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<String, List<Document>> entry = (Entry)var4.next();
         List<Document> list = (List)entry.getValue();

         assert list.size() == 1 || list.size() == 2 : "Chunk min/max filter should be of size 1 or 2, but got size " + list.size();

         if (list.size() == 1) {
            listToAnd.addAll(list);
         } else {
            String fieldName = (String)entry.getKey();
            Document rangeQuery = new Document();
            Document bound = (Document)((Document)list.get(0)).get(fieldName);
            rangeQuery.putAll(bound);
            bound = (Document)((Document)list.get(1)).get(fieldName);
            rangeQuery.putAll(bound);
            listToAnd.add(new Document(fieldName, rangeQuery));
         }
      }

      if (pushdownFilters != null && !pushdownFilters.isEmpty()) {
         listToAnd.add(pushdownFilters);
      }

      if (listToAnd.size() > 0) {
         Document andQueryFilter = new Document();
         andQueryFilter.put(MongoFunctions.AND.getMongoOperator(), listToAnd);
         toReturn = andQueryFilter;
      }

      return toReturn;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("find(");
      sb.append(this.match == null ? "{}" : this.match.toJson());
      sb.append(", ");
      sb.append(this.project == null ? "{}" : this.project.toJson());
      sb.append(")");
      return sb.toString();
   }

   public boolean equals(Object other) {
      if (!(other instanceof MongoPipeline)) {
         return false;
      } else {
         MongoPipeline that = (MongoPipeline)other;
         return Objects.equals(this.match, that.match) && Objects.equals(this.project, that.project);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.match, this.project});
   }

   public MongoPipeline copy() {
      return new MongoPipeline(this.match, this.project, this.needsCollation);
   }

   public MongoPipeline newWithoutProject() {
      return new MongoPipeline(this.match, (Document)null, this.needsCollation);
   }
}
