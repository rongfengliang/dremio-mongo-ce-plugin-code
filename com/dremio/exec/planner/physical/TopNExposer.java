package com.dremio.exec.planner.physical;

import org.apache.calcite.rel.RelCollation;

public class TopNExposer {
   public static RelCollation getCollation(TopNPrel prel) {
      return prel.collation;
   }

   public static int getLimit(TopNPrel prel) {
      return prel.limit;
   }
}
