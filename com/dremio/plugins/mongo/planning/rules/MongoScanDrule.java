package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.mongo.MongoConf;
import com.dremio.plugins.mongo.planning.rels.MongoColumnNameSanitizer;
import com.dremio.plugins.mongo.planning.rels.MongoScanDrel;

public class MongoScanDrule extends SourceLogicalConverter {
   public static final MongoScanDrule INSTANCE = new MongoScanDrule();

   private MongoScanDrule() {
      super((SourceType)MongoConf.class.getAnnotation(SourceType.class));
   }

   public Rel convertScan(ScanCrel scan) {
      return new MongoScanDrel(scan.getCluster(), scan.getTraitSet().plus(Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), MongoColumnNameSanitizer.sanitizeColumnNames(scan.getRowType()).getFieldNames(), scan.getObservedRowcountAdjustment());
   }
}
