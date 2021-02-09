package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.plugins.mongo.planning.rules.MongoImplementor;

public interface MongoRel extends Prel {
   MongoScanSpec implement(MongoImplementor var1);

   BatchSchema getSchema(FunctionLookupContext var1);
}
