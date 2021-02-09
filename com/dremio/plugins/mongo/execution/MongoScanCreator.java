package com.dremio.plugins.mongo.execution;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.store.CoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.plugins.mongo.MongoStoragePlugin;
import com.dremio.plugins.mongo.connection.MongoConnectionManager;
import com.dremio.plugins.mongo.planning.MongoSubScan;
import com.dremio.plugins.mongo.planning.MongoSubScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import java.util.List;

public class MongoScanCreator implements Creator<MongoSubScan> {
   public ProducerOperator create(FragmentExecutionContext fec, final OperatorContext context, final MongoSubScan subScan) throws ExecutionSetupException {
      final List<SchemaPath> columns = subScan.getColumns() == null ? GroupScan.ALL_COLUMNS : subScan.getColumns();
      final int batchSize = context.getTargetBatchSize();
      Preconditions.checkArgument(!subScan.isSingleFragment() || subScan.getChunkScanSpecList().size() == 1);
      final boolean isSingleFragment = subScan.isSingleFragment();
      MongoStoragePlugin plugin = (MongoStoragePlugin)fec.getStoragePlugin(subScan.getPluginId());
      final MongoConnectionManager manager = plugin.getManager();
      Iterable<RecordReader> readers = FluentIterable.from(subScan.getChunkScanSpecList()).transform(new Function<MongoSubScanSpec, RecordReader>() {
         public RecordReader apply(MongoSubScanSpec scanSpec) {
            MongoRecordReader innerReader = new MongoRecordReader(manager, context, scanSpec, (List)columns, subScan.getSanitizedColumns(), context.getManagedBuffer(), isSingleFragment, batchSize, subScan.getFullSchema());
            return (RecordReader)(!scanSpec.getPipeline().needsCoercion() ? innerReader : new CoercionReader(context, (List)columns, innerReader, subScan.getFullSchema()));
         }
      });
      return new ScanOperator(subScan, context, RecordReaderIterator.from(readers.iterator()));
   }
}
