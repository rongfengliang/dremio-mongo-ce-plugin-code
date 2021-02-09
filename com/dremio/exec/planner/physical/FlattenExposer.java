package com.dremio.exec.planner.physical;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class FlattenExposer {
   public static Integer getInputRef(FlattenPrel prel) {
      RexNode node = prel.toFlatten;
      return node instanceof RexInputRef ? ((RexInputRef)node).getIndex() : null;
   }
}
