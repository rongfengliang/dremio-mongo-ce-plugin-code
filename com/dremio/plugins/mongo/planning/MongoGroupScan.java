package com.dremio.plugins.mongo.planning;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;

public class MongoGroupScan extends AbstractGroupScan {
   private static final double[] REDUCTION_FACTOR = new double[]{1.0D, 0.2D, 0.1D, 0.05D};
   private final MongoScanSpec spec;
   private final long rowCountEstimate;
   private final boolean isSingleFragment;
   private final BatchSchema schema;
   private final List<SchemaPath> sanitizedColumns;

   public MongoGroupScan(OpProps props, MongoScanSpec spec, TableMetadata table, List<SchemaPath> columns, List<SchemaPath> sanitizedColumns, BatchSchema schema, long rowCountEstimate, boolean isSingleFragment) {
      super(props, table, columns);
      this.spec = spec;
      this.rowCountEstimate = rowCountEstimate;
      this.isSingleFragment = isSingleFragment;
      this.schema = schema;
      this.sanitizedColumns = sanitizedColumns;
   }

   public MongoScanSpec getScanSpec() {
      return this.spec;
   }

   public int getMaxParallelizationWidth() {
      return this.isSingleFragment ? 1 : super.getMaxParallelizationWidth();
   }

   public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
      ImmutableList splitWork;
      if (this.isSingleFragment) {
         splitWork = ImmutableList.of(new MongoSubScanSpec(this.spec.getDbName(), this.spec.getCollectionName(), ImmutableList.of(), (String)null, (String)null, this.spec.getPipeline().copy()));
      } else {
         splitWork = FluentIterable.from(work).transform(new Function<SplitWork, MongoSubScanSpec>() {
            public MongoSubScanSpec apply(SplitWork input) {
               return MongoSubScanSpec.of(MongoGroupScan.this.spec, input.getSplitAndPartitionInfo());
            }
         }).toList();
      }

      return new MongoSubScan(this.props, this.getDataset().getStoragePluginId(), splitWork, (List)(this.spec.getPipeline().isSimpleScan() ? GroupScan.ALL_COLUMNS : this.getColumns()), this.sanitizedColumns, this.isSingleFragment, (List)Iterables.getOnlyElement(this.getReferencedTables()), this.props.getSchema());
   }

   public int getOperatorType() {
      return 37;
   }

   public boolean equals(Object other) {
      if (!(other instanceof MongoGroupScan)) {
         return false;
      } else {
         MongoGroupScan castOther = (MongoGroupScan)other;
         return Objects.equal(this.spec, castOther.spec) && Objects.equal(this.rowCountEstimate, castOther.rowCountEstimate);
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.spec, this.rowCountEstimate});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("spec", this.spec).add("rowCountEstimate", this.rowCountEstimate).toString();
   }
}
