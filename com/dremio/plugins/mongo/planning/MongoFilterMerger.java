package com.dremio.plugins.mongo.planning;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.bson.Document;

public class MongoFilterMerger {
   public static Map<String, List<Document>> mergeFilters(Document minFilters, Document maxFilters) {
      Map<String, List<Document>> filters = Maps.newHashMap();
      Iterator var3;
      Entry entry;
      Object list;
      if (minFilters != null) {
         for(var3 = minFilters.entrySet().iterator(); var3.hasNext(); ((List)list).add(new Document((String)entry.getKey(), new Document(MongoFunctions.GREATER_OR_EQUAL.getMongoOperator(), entry.getValue())))) {
            entry = (Entry)var3.next();
            list = (List)filters.get(entry.getKey());
            if (list == null) {
               list = Lists.newArrayList();
               filters.put((String)entry.getKey(), list);
            }
         }
      }

      if (maxFilters != null) {
         for(var3 = maxFilters.entrySet().iterator(); var3.hasNext(); ((List)list).add(new Document((String)entry.getKey(), new Document(MongoFunctions.LESS.getMongoOperator(), entry.getValue())))) {
            entry = (Entry)var3.next();
            list = (List)filters.get(entry.getKey());
            if (list == null) {
               list = Lists.newArrayList();
               filters.put((String)entry.getKey(), list);
            }
         }
      }

      return filters;
   }
}
