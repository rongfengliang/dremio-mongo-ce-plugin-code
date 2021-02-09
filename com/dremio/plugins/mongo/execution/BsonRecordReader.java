package com.dremio.plugins.mongo.execution;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.plugins.mongo.planning.rels.MongoColumnNameSanitizer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BsonRecordReader {
   static final Logger logger = LoggerFactory.getLogger(BsonRecordReader.class);
   private final FieldSelection fieldSelection;
   private final int maxFieldSize;
   private final int maxLeafLimit;
   private final boolean readNumbersAsDouble;
   protected ArrowBuf workBuf;
   private final Map<String, Integer> fieldDecimalScale;
   protected int currentLeafCount;

   public BsonRecordReader(ArrowBuf managedBuf, int maxFieldSize, int maxLeafLimit, boolean allTextMode, boolean readNumbersAsDouble) {
      this(managedBuf, GroupScan.ALL_COLUMNS, maxFieldSize, maxLeafLimit, readNumbersAsDouble, (Map)null);
   }

   public BsonRecordReader(ArrowBuf managedBuf, List<SchemaPath> sanitizedColumns, int maxFieldSize, int maxLeafLimit, boolean readNumbersAsDouble, Map<String, Integer> decimalScales) {
      assert ((List)Preconditions.checkNotNull(sanitizedColumns)).size() > 0 : "bson record reader requires at least a column";

      this.maxFieldSize = maxFieldSize;
      this.maxLeafLimit = maxLeafLimit;
      this.readNumbersAsDouble = readNumbersAsDouble;
      this.workBuf = managedBuf;
      this.fieldSelection = FieldSelection.getFieldSelection(sanitizedColumns);
      this.fieldDecimalScale = (Map)(null == decimalScales ? new HashMap() : decimalScales);
      this.currentLeafCount = 0;
   }

   public void write(ComplexWriter writer, BsonReader reader) throws IOException {
      this.currentLeafCount = 0;
      reader.readStartDocument();
      BsonType readBsonType = reader.getCurrentBsonType();
      switch(readBsonType) {
      case DOCUMENT:
         this.writeToListOrMap(reader, (FieldWriter)writer.rootAsStruct(), false, (String)null, this.fieldSelection);
         return;
      default:
         throw new RuntimeException("Root object must be DOCUMENT type. Found: " + readBsonType);
      }
   }

   private void writeToListOrMap(BsonReader reader, FieldWriter writer, boolean isList, String fieldName, FieldSelection fieldSelection) {
      if (isList) {
         writer.startList();
      } else {
         writer.start();
      }

      int originalLeafCount = this.currentLeafCount;
      int maxArrayLeafCount = 0;

      while(true) {
         while(reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (!isList) {
               fieldName = reader.readName();
            }

            BsonType currentBsonType = reader.getCurrentBsonType();
            FieldSelection childFieldSelection = fieldSelection.getChild(fieldName);
            if (childFieldSelection.isNeverValid() && fieldName.equals("_id")) {
               reader.skipValue();
            } else {
               fieldName = MongoColumnNameSanitizer.unsanitizeColumnName(fieldName);
               if (isList) {
                  this.currentLeafCount = originalLeafCount;
               }

               switch(currentBsonType) {
               case DOCUMENT:
                  reader.readStartDocument();
                  FieldWriter _writer;
                  if (!isList) {
                     _writer = (FieldWriter)writer.struct(fieldName);
                  } else {
                     _writer = (FieldWriter)writer.struct();
                  }

                  this.writeToListOrMap(reader, _writer, false, fieldName, childFieldSelection);
                  break;
               case INT32:
                  int readInt32 = reader.readInt32();
                  if (this.readNumbersAsDouble) {
                     this.writeDouble((double)readInt32, writer, fieldName, isList);
                  } else {
                     this.writeInt32(readInt32, writer, fieldName, isList);
                  }
                  break;
               case INT64:
                  long readInt64 = reader.readInt64();
                  if (this.readNumbersAsDouble) {
                     this.writeDouble((double)readInt64, writer, fieldName, isList);
                  } else {
                     this.writeInt64(readInt64, writer, fieldName, isList);
                  }
                  break;
               case ARRAY:
                  reader.readStartArray();
                  if (isList) {
                     this.writeToListOrMap(reader, (FieldWriter)writer.list(), true, fieldName, childFieldSelection);
                  } else {
                     this.writeToListOrMap(reader, (FieldWriter)writer.list(fieldName), true, fieldName, childFieldSelection);
                  }
                  break;
               case BINARY:
                  this.writeBinary(reader, writer, fieldName, isList);
                  break;
               case BOOLEAN:
                  boolean readBoolean = reader.readBoolean();
                  this.writeBoolean(readBoolean, writer, fieldName, isList);
                  break;
               case DATE_TIME:
                  long readDateTime = reader.readDateTime();
                  this.writeDateTime(readDateTime, writer, fieldName, isList);
                  break;
               case DECIMAL128:
                  Decimal128 decimal = reader.readDecimal128();
                  this.writeDecimal(decimal.bigDecimalValue(), writer, fieldName, isList);
                  break;
               case DOUBLE:
                  double readDouble = reader.readDouble();
                  this.writeDouble(readDouble, writer, fieldName, isList);
                  break;
               case JAVASCRIPT:
                  String readJavaScript = reader.readJavaScript();
                  this.writeString(readJavaScript, writer, fieldName, isList);
                  break;
               case JAVASCRIPT_WITH_SCOPE:
                  this.writeJavaScriptWithScope(reader, isList ? writer.struct() : writer.struct(fieldName));
                  break;
               case NULL:
                  reader.readNull();
                  break;
               case OBJECT_ID:
                  this.writeObjectId(reader, writer, fieldName, isList);
                  break;
               case STRING:
                  String readString = reader.readString();
                  this.writeString(readString, writer, fieldName, isList);
                  break;
               case SYMBOL:
                  String readSymbol = reader.readSymbol();
                  this.writeString(readSymbol, writer, fieldName, isList);
                  break;
               case TIMESTAMP:
                  int time = reader.readTimestamp().getTime();
                  this.writeTimeStampMilli(time, writer, fieldName, isList);
                  break;
               case DB_POINTER:
                  this.writeDbPointer(reader.readDBPointer(), isList ? writer.struct() : writer.struct(fieldName));
                  break;
               case REGULAR_EXPRESSION:
                  StructWriter structWriter = isList ? writer.struct() : writer.struct(fieldName);
                  BsonRegularExpression regex = reader.readRegularExpression();
                  structWriter.start();
                  this.writeString(regex.getPattern(), (FieldWriter)structWriter, "pattern", false);
                  this.writeString(regex.getOptions(), (FieldWriter)structWriter, "options", false);
                  structWriter.end();
                  break;
               case MIN_KEY:
                  reader.readMinKey();
                  break;
               case MAX_KEY:
                  reader.readMaxKey();
                  break;
               default:
                  throw new IllegalArgumentException("UnSupported Bson type: " + currentBsonType);
               }

               if (isList) {
                  maxArrayLeafCount = Math.max(maxArrayLeafCount, this.currentLeafCount);
               }
            }
         }

         if (!isList) {
            reader.readEndDocument();
            writer.end();
         } else {
            this.currentLeafCount = maxArrayLeafCount;
            reader.readEndArray();
            writer.endList();
         }

         return;
      }
   }

   private void writeDbPointer(BsonDbPointer dbPointer, StructWriter structWriter) {
      this.writeString(dbPointer.getNamespace(), (FieldWriter)structWriter, "namespace", false);
      this.incrementLeafCount();
      byte[] objBytes = dbPointer.getId().toByteArray();
      this.ensure(objBytes.length);
      this.workBuf.setBytes(0L, objBytes);
      structWriter.varBinary("id").writeVarBinary(0, objBytes.length, this.workBuf);
   }

   private void writeJavaScriptWithScope(BsonReader reader, StructWriter writer) {
      writer.start();
      this.writeString(reader.readJavaScriptWithScope(), (FieldWriter)writer, "code", false);
      reader.readStartDocument();
      FieldWriter scopeWriter = (FieldWriter)writer.struct("scope");
      this.writeToListOrMap(reader, scopeWriter, false, "scope", FieldSelection.ALL_VALID);
      writer.end();
   }

   private void writeBinary(BsonReader reader, FieldWriter writer, String fieldName, boolean isList) {
      VarBinaryHolder vb = new VarBinaryHolder();
      FieldSizeLimitExceptionHelper.checkSizeLimit(reader.peekBinarySize(), this.maxFieldSize, fieldName, logger);
      BsonBinary readBinaryData = reader.readBinaryData();
      byte[] data = readBinaryData.getData();
      Byte type = readBinaryData.getType();
      switch(type.intValue()) {
      case 1:
         this.writeDouble(ByteBuffer.wrap(data).getDouble(), writer, fieldName, isList);
         break;
      case 2:
         this.writeString(new String(data), writer, fieldName, isList);
         break;
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
      case 10:
      case 11:
      case 12:
      default:
         byte[] bytes = readBinaryData.getData();
         this.writeBinary(writer, fieldName, isList, vb, bytes);
         break;
      case 8:
         boolean boolValue = data != null && data.length != 0 ? data[0] != 0 : false;
         this.writeBoolean(boolValue, writer, fieldName, isList);
         break;
      case 9:
         this.writeDateTime(ByteBuffer.wrap(data).getLong(), writer, fieldName, isList);
         break;
      case 13:
         this.writeString(new String(data), writer, fieldName, isList);
         break;
      case 14:
         this.writeString(new String(data), writer, fieldName, isList);
         break;
      case 15:
         this.writeString(new String(data), writer, fieldName, isList);
         break;
      case 16:
         this.writeInt32(ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
         break;
      case 17:
         this.writeTimeStampMilli(ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
         break;
      case 18:
         this.writeInt64((long)ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
      }

   }

   private void writeTimeStampMilli(int timestamp, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      TimeStampMilliWriter t;
      if (!isList) {
         t = writer.timeStampMilli(fieldName);
      } else {
         t = writer.timeStampMilli();
      }

      t.writeTimeStampMilli((long)timestamp * 1000L);
   }

   private void writeString(String readString, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      byte[] readStringBytes = readString.getBytes(StandardCharsets.UTF_8);
      FieldSizeLimitExceptionHelper.checkSizeLimit(readStringBytes.length, this.maxFieldSize, fieldName, logger);
      this.ensure(readStringBytes.length);
      this.workBuf.setBytes(0L, readStringBytes);
      if (!isList) {
         writer.varChar(fieldName).writeVarChar(0, readStringBytes.length, this.workBuf);
      } else {
         writer.varChar().writeVarChar(0, readStringBytes.length, this.workBuf);
      }

   }

   private void writeObjectId(BsonReader reader, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      VarBinaryHolder vObj = new VarBinaryHolder();
      byte[] objBytes = reader.readObjectId().toByteArray();
      this.writeBinary(writer, fieldName, isList, vObj, objBytes);
   }

   private void writeDecimal(BigDecimal readDecimal, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      if (readDecimal.precision() > 38 && readDecimal.scale() > 6) {
         readDecimal = readDecimal.setScale(Math.max(6, readDecimal.scale() - (readDecimal.precision() - 38)), 5);
      }

      Integer scale = (Integer)this.fieldDecimalScale.get(fieldName);
      if (scale == null) {
         this.fieldDecimalScale.put(fieldName, readDecimal.scale());
      } else if (readDecimal.scale() < scale) {
         readDecimal = readDecimal.setScale(scale);
      } else if (readDecimal.scale() > scale) {
         logger.debug("Scale change detected on field {}, from {} to {}", new Object[]{fieldName, scale, readDecimal.scale()});
         this.fieldDecimalScale.put(fieldName, readDecimal.scale());
         throw new BsonRecordReader.ChangedScaleException();
      }

      if (!isList) {
         writer.decimal(fieldName, readDecimal.scale(), 38).writeDecimal(readDecimal);
      } else {
         writer.decimal().writeDecimal(readDecimal);
      }

   }

   private void writeDouble(double readDouble, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      if (!isList) {
         writer.float8(fieldName).writeFloat8(readDouble);
      } else {
         writer.float8().writeFloat8(readDouble);
      }

   }

   private void writeDateTime(long readDateTime, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      TimeStampMilliWriter dt;
      if (!isList) {
         dt = writer.timeStampMilli(fieldName);
      } else {
         dt = writer.timeStampMilli();
      }

      dt.writeTimeStampMilli(readDateTime);
   }

   private void writeBoolean(boolean readBoolean, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      BitHolder bit = new BitHolder();
      bit.value = readBoolean ? 1 : 0;
      if (!isList) {
         writer.bit(fieldName).writeBit(bit.value);
      } else {
         writer.bit().writeBit(bit.value);
      }

   }

   private void writeBinary(FieldWriter writer, String fieldName, boolean isList, VarBinaryHolder vb, byte[] bytes) {
      this.incrementLeafCount();
      FieldSizeLimitExceptionHelper.checkSizeLimit(bytes.length, this.maxFieldSize, fieldName, logger);
      this.ensure(bytes.length);
      this.workBuf.setBytes(0L, bytes);
      vb.buffer = this.workBuf;
      vb.start = 0;
      vb.end = bytes.length;
      if (!isList) {
         writer.varBinary(fieldName).writeVarBinary(vb.start, vb.end, vb.buffer);
      } else {
         writer.varBinary().writeVarBinary(vb.start, vb.end, vb.buffer);
      }

   }

   private void writeInt64(long readInt64, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      if (!isList) {
         writer.bigInt(fieldName).writeBigInt(readInt64);
      } else {
         writer.bigInt().writeBigInt(readInt64);
      }

   }

   private void writeInt32(int readInt32, FieldWriter writer, String fieldName, boolean isList) {
      this.incrementLeafCount();
      if (!isList) {
         writer.integer(fieldName).writeInt(readInt32);
      } else {
         writer.integer().writeInt(readInt32);
      }

   }

   private void ensure(int length) {
      this.workBuf = this.workBuf.reallocIfNeeded((long)length);
   }

   Map<String, Integer> getDecimalScales() {
      return this.fieldDecimalScale;
   }

   private void incrementLeafCount() {
      if (++this.currentLeafCount > this.maxLeafLimit) {
         throw new ColumnCountTooLargeException(this.maxLeafLimit);
      }
   }

   public static class ChangedScaleException extends RuntimeException {
   }
}
