package com.dremio.mongo.proto;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.LazyStringArrayList;
import com.google.protobuf.LazyStringList;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.GeneratedMessageV3.BuilderParent;
import com.google.protobuf.GeneratedMessageV3.FieldAccessorTable;
import com.google.protobuf.GeneratedMessageV3.UnusedPrivateParameter;
import com.google.protobuf.Internal.EnumLiteMap;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

public final class MongoReaderProto {
   private static final Descriptor internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
   private static final FieldAccessorTable internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable;
   private static final Descriptor internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
   private static final FieldAccessorTable internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable;
   private static FileDescriptor descriptor;

   private MongoReaderProto() {
   }

   public static void registerAllExtensions(ExtensionRegistryLite registry) {
   }

   public static void registerAllExtensions(ExtensionRegistry registry) {
      registerAllExtensions((ExtensionRegistryLite)registry);
   }

   public static FileDescriptor getDescriptor() {
      return descriptor;
   }

   static {
      String[] descriptorData = new String[]{"\n\u000bmongo.proto\u0012\u0018com.dremio.elastic.proto\"o\n\u000fMongoTableXattr\u0012\u0010\n\bdatabase\u0018\u0001 \u0001(\t\u0012\u0012\n\ncollection\u0018\u0002 \u0001(\t\u00126\n\u0004type\u0018\u0003 \u0001(\u000e2(.com.dremio.elastic.proto.CollectionType\"H\n\u000fMongoSplitXattr\u0012\u0012\n\nmin_filter\u0018\u0001 \u0001(\t\u0012\u0012\n\nmax_filter\u0018\u0002 \u0001(\t\u0012\r\n\u0005hosts\u0018\u0003 \u0003(\t*d\n\u000eCollectionType\u0012\u0014\n\u0010SINGLE_PARTITION\u0010\u0001\u0012\u0013\n\u000fSUB_PARTITIONED\u0010\u0002\u0012\u0012\n\u000eNODE_PARTITION\u0010\u0003\u0012\u0013\n\u000fRANGE_PARTITION\u0010\u0004B,\n\u0016com.dremio.mongo.protoB\u0010MongoReaderProtoH\u0001"};
      descriptor = FileDescriptor.internalBuildGeneratedFileFrom(descriptorData, new FileDescriptor[0]);
      internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor = (Descriptor)getDescriptor().getMessageTypes().get(0);
      internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable = new FieldAccessorTable(internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor, new String[]{"Database", "Collection", "Type"});
      internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor = (Descriptor)getDescriptor().getMessageTypes().get(1);
      internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable = new FieldAccessorTable(internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor, new String[]{"MinFilter", "MaxFilter", "Hosts"});
   }

   public static final class MongoSplitXattr extends GeneratedMessageV3 implements MongoReaderProto.MongoSplitXattrOrBuilder {
      private static final long serialVersionUID = 0L;
      private int bitField0_;
      public static final int MIN_FILTER_FIELD_NUMBER = 1;
      private volatile Object minFilter_;
      public static final int MAX_FILTER_FIELD_NUMBER = 2;
      private volatile Object maxFilter_;
      public static final int HOSTS_FIELD_NUMBER = 3;
      private LazyStringList hosts_;
      private byte memoizedIsInitialized;
      private static final MongoReaderProto.MongoSplitXattr DEFAULT_INSTANCE = new MongoReaderProto.MongoSplitXattr();
      /** @deprecated */
      @Deprecated
      public static final Parser<MongoReaderProto.MongoSplitXattr> PARSER = new AbstractParser<MongoReaderProto.MongoSplitXattr>() {
         public MongoReaderProto.MongoSplitXattr parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return new MongoReaderProto.MongoSplitXattr(input, extensionRegistry);
         }
      };

      private MongoSplitXattr(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
         super(builder);
         this.memoizedIsInitialized = -1;
      }

      private MongoSplitXattr() {
         this.memoizedIsInitialized = -1;
         this.minFilter_ = "";
         this.maxFilter_ = "";
         this.hosts_ = LazyStringArrayList.EMPTY;
      }

      protected Object newInstance(UnusedPrivateParameter unused) {
         return new MongoReaderProto.MongoSplitXattr();
      }

      public final UnknownFieldSet getUnknownFields() {
         return this.unknownFields;
      }

      private MongoSplitXattr(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         this();
         if (extensionRegistry == null) {
            throw new NullPointerException();
         } else {
            int mutable_bitField0_ = 0;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();

            try {
               boolean done = false;

               while(!done) {
                  int tag = input.readTag();
                  ByteString bs;
                  switch(tag) {
                  case 0:
                     done = true;
                     break;
                  case 10:
                     bs = input.readBytes();
                     this.bitField0_ |= 1;
                     this.minFilter_ = bs;
                     break;
                  case 18:
                     bs = input.readBytes();
                     this.bitField0_ |= 2;
                     this.maxFilter_ = bs;
                     break;
                  case 26:
                     bs = input.readBytes();
                     if ((mutable_bitField0_ & 4) == 0) {
                        this.hosts_ = new LazyStringArrayList();
                        mutable_bitField0_ |= 4;
                     }

                     this.hosts_.add(bs);
                     break;
                  default:
                     if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                        done = true;
                     }
                  }
               }
            } catch (InvalidProtocolBufferException var12) {
               throw var12.setUnfinishedMessage(this);
            } catch (IOException var13) {
               throw (new InvalidProtocolBufferException(var13)).setUnfinishedMessage(this);
            } finally {
               if ((mutable_bitField0_ & 4) != 0) {
                  this.hosts_ = this.hosts_.getUnmodifiableView();
               }

               this.unknownFields = unknownFields.build();
               this.makeExtensionsImmutable();
            }

         }
      }

      public static final Descriptor getDescriptor() {
         return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
      }

      protected FieldAccessorTable internalGetFieldAccessorTable() {
         return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoReaderProto.MongoSplitXattr.class, MongoReaderProto.MongoSplitXattr.Builder.class);
      }

      public boolean hasMinFilter() {
         return (this.bitField0_ & 1) != 0;
      }

      public String getMinFilter() {
         Object ref = this.minFilter_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.minFilter_ = s;
            }

            return s;
         }
      }

      public ByteString getMinFilterBytes() {
         Object ref = this.minFilter_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.minFilter_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public boolean hasMaxFilter() {
         return (this.bitField0_ & 2) != 0;
      }

      public String getMaxFilter() {
         Object ref = this.maxFilter_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.maxFilter_ = s;
            }

            return s;
         }
      }

      public ByteString getMaxFilterBytes() {
         Object ref = this.maxFilter_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.maxFilter_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public ProtocolStringList getHostsList() {
         return this.hosts_;
      }

      public int getHostsCount() {
         return this.hosts_.size();
      }

      public String getHosts(int index) {
         return (String)this.hosts_.get(index);
      }

      public ByteString getHostsBytes(int index) {
         return this.hosts_.getByteString(index);
      }

      public final boolean isInitialized() {
         byte isInitialized = this.memoizedIsInitialized;
         if (isInitialized == 1) {
            return true;
         } else if (isInitialized == 0) {
            return false;
         } else {
            this.memoizedIsInitialized = 1;
            return true;
         }
      }

      public void writeTo(CodedOutputStream output) throws IOException {
         if ((this.bitField0_ & 1) != 0) {
            GeneratedMessageV3.writeString(output, 1, this.minFilter_);
         }

         if ((this.bitField0_ & 2) != 0) {
            GeneratedMessageV3.writeString(output, 2, this.maxFilter_);
         }

         for(int i = 0; i < this.hosts_.size(); ++i) {
            GeneratedMessageV3.writeString(output, 3, this.hosts_.getRaw(i));
         }

         this.unknownFields.writeTo(output);
      }

      public int getSerializedSize() {
         int size = this.memoizedSize;
         if (size != -1) {
            return size;
         } else {
            size = 0;
            if ((this.bitField0_ & 1) != 0) {
               size += GeneratedMessageV3.computeStringSize(1, this.minFilter_);
            }

            if ((this.bitField0_ & 2) != 0) {
               size += GeneratedMessageV3.computeStringSize(2, this.maxFilter_);
            }

            int dataSize = 0;

            for(int i = 0; i < this.hosts_.size(); ++i) {
               dataSize += computeStringSizeNoTag(this.hosts_.getRaw(i));
            }

            size += dataSize;
            size += 1 * this.getHostsList().size();
            size += this.unknownFields.getSerializedSize();
            this.memoizedSize = size;
            return size;
         }
      }

      public boolean equals(Object obj) {
         if (obj == this) {
            return true;
         } else if (!(obj instanceof MongoReaderProto.MongoSplitXattr)) {
            return super.equals(obj);
         } else {
            MongoReaderProto.MongoSplitXattr other = (MongoReaderProto.MongoSplitXattr)obj;
            if (this.hasMinFilter() != other.hasMinFilter()) {
               return false;
            } else if (this.hasMinFilter() && !this.getMinFilter().equals(other.getMinFilter())) {
               return false;
            } else if (this.hasMaxFilter() != other.hasMaxFilter()) {
               return false;
            } else if (this.hasMaxFilter() && !this.getMaxFilter().equals(other.getMaxFilter())) {
               return false;
            } else if (!this.getHostsList().equals(other.getHostsList())) {
               return false;
            } else {
               return this.unknownFields.equals(other.unknownFields);
            }
         }
      }

      public int hashCode() {
         if (this.memoizedHashCode != 0) {
            return this.memoizedHashCode;
         } else {
            int hash = 41;
            int hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasMinFilter()) {
               hash = 37 * hash + 1;
               hash = 53 * hash + this.getMinFilter().hashCode();
            }

            if (this.hasMaxFilter()) {
               hash = 37 * hash + 2;
               hash = 53 * hash + this.getMaxFilter().hashCode();
            }

            if (this.getHostsCount() > 0) {
               hash = 37 * hash + 3;
               hash = 53 * hash + this.getHostsList().hashCode();
            }

            hash = 29 * hash + this.unknownFields.hashCode();
            this.memoizedHashCode = hash;
            return hash;
         }
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(ByteBuffer data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(ByteString data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(byte[] data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoSplitXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(InputStream input) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public static MongoReaderProto.MongoSplitXattr parseDelimitedFrom(InputStream input) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoSplitXattr parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(CodedInputStream input) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoSplitXattr parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoSplitXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public MongoReaderProto.MongoSplitXattr.Builder newBuilderForType() {
         return newBuilder();
      }

      public static MongoReaderProto.MongoSplitXattr.Builder newBuilder() {
         return DEFAULT_INSTANCE.toBuilder();
      }

      public static MongoReaderProto.MongoSplitXattr.Builder newBuilder(MongoReaderProto.MongoSplitXattr prototype) {
         return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
      }

      public MongoReaderProto.MongoSplitXattr.Builder toBuilder() {
         return this == DEFAULT_INSTANCE ? new MongoReaderProto.MongoSplitXattr.Builder() : (new MongoReaderProto.MongoSplitXattr.Builder()).mergeFrom(this);
      }

      protected MongoReaderProto.MongoSplitXattr.Builder newBuilderForType(BuilderParent parent) {
         MongoReaderProto.MongoSplitXattr.Builder builder = new MongoReaderProto.MongoSplitXattr.Builder(parent);
         return builder;
      }

      public static MongoReaderProto.MongoSplitXattr getDefaultInstance() {
         return DEFAULT_INSTANCE;
      }

      public static Parser<MongoReaderProto.MongoSplitXattr> parser() {
         return PARSER;
      }

      public Parser<MongoReaderProto.MongoSplitXattr> getParserForType() {
         return PARSER;
      }

      public MongoReaderProto.MongoSplitXattr getDefaultInstanceForType() {
         return DEFAULT_INSTANCE;
      }

      // $FF: synthetic method
      MongoSplitXattr(com.google.protobuf.GeneratedMessageV3.Builder x0, Object x1) {
         this(x0);
      }

      // $FF: synthetic method
      MongoSplitXattr(CodedInputStream x0, ExtensionRegistryLite x1, Object x2) throws InvalidProtocolBufferException {
         this(x0, x1);
      }

      public static final class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<MongoReaderProto.MongoSplitXattr.Builder> implements MongoReaderProto.MongoSplitXattrOrBuilder {
         private int bitField0_;
         private Object minFilter_;
         private Object maxFilter_;
         private LazyStringList hosts_;

         public static final Descriptor getDescriptor() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
         }

         protected FieldAccessorTable internalGetFieldAccessorTable() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoReaderProto.MongoSplitXattr.class, MongoReaderProto.MongoSplitXattr.Builder.class);
         }

         private Builder() {
            this.minFilter_ = "";
            this.maxFilter_ = "";
            this.hosts_ = LazyStringArrayList.EMPTY;
            this.maybeForceBuilderInitialization();
         }

         private Builder(BuilderParent parent) {
            super(parent);
            this.minFilter_ = "";
            this.maxFilter_ = "";
            this.hosts_ = LazyStringArrayList.EMPTY;
            this.maybeForceBuilderInitialization();
         }

         private void maybeForceBuilderInitialization() {
            if (MongoReaderProto.MongoSplitXattr.alwaysUseFieldBuilders) {
            }

         }

         public MongoReaderProto.MongoSplitXattr.Builder clear() {
            super.clear();
            this.minFilter_ = "";
            this.bitField0_ &= -2;
            this.maxFilter_ = "";
            this.bitField0_ &= -3;
            this.hosts_ = LazyStringArrayList.EMPTY;
            this.bitField0_ &= -5;
            return this;
         }

         public Descriptor getDescriptorForType() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
         }

         public MongoReaderProto.MongoSplitXattr getDefaultInstanceForType() {
            return MongoReaderProto.MongoSplitXattr.getDefaultInstance();
         }

         public MongoReaderProto.MongoSplitXattr build() {
            MongoReaderProto.MongoSplitXattr result = this.buildPartial();
            if (!result.isInitialized()) {
               throw newUninitializedMessageException(result);
            } else {
               return result;
            }
         }

         public MongoReaderProto.MongoSplitXattr buildPartial() {
            MongoReaderProto.MongoSplitXattr result = new MongoReaderProto.MongoSplitXattr(this);
            int from_bitField0_ = this.bitField0_;
            int to_bitField0_ = 0;
            if ((from_bitField0_ & 1) != 0) {
               to_bitField0_ |= 1;
            }

            result.minFilter_ = this.minFilter_;
            if ((from_bitField0_ & 2) != 0) {
               to_bitField0_ |= 2;
            }

            result.maxFilter_ = this.maxFilter_;
            if ((this.bitField0_ & 4) != 0) {
               this.hosts_ = this.hosts_.getUnmodifiableView();
               this.bitField0_ &= -5;
            }

            result.hosts_ = this.hosts_;
            result.bitField0_ = to_bitField0_;
            this.onBuilt();
            return result;
         }

         public MongoReaderProto.MongoSplitXattr.Builder clone() {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.clone();
         }

         public MongoReaderProto.MongoSplitXattr.Builder setField(FieldDescriptor field, Object value) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.setField(field, value);
         }

         public MongoReaderProto.MongoSplitXattr.Builder clearField(FieldDescriptor field) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.clearField(field);
         }

         public MongoReaderProto.MongoSplitXattr.Builder clearOneof(OneofDescriptor oneof) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.clearOneof(oneof);
         }

         public MongoReaderProto.MongoSplitXattr.Builder setRepeatedField(FieldDescriptor field, int index, Object value) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.setRepeatedField(field, index, value);
         }

         public MongoReaderProto.MongoSplitXattr.Builder addRepeatedField(FieldDescriptor field, Object value) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.addRepeatedField(field, value);
         }

         public MongoReaderProto.MongoSplitXattr.Builder mergeFrom(Message other) {
            if (other instanceof MongoReaderProto.MongoSplitXattr) {
               return this.mergeFrom((MongoReaderProto.MongoSplitXattr)other);
            } else {
               super.mergeFrom(other);
               return this;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder mergeFrom(MongoReaderProto.MongoSplitXattr other) {
            if (other == MongoReaderProto.MongoSplitXattr.getDefaultInstance()) {
               return this;
            } else {
               if (other.hasMinFilter()) {
                  this.bitField0_ |= 1;
                  this.minFilter_ = other.minFilter_;
                  this.onChanged();
               }

               if (other.hasMaxFilter()) {
                  this.bitField0_ |= 2;
                  this.maxFilter_ = other.maxFilter_;
                  this.onChanged();
               }

               if (!other.hosts_.isEmpty()) {
                  if (this.hosts_.isEmpty()) {
                     this.hosts_ = other.hosts_;
                     this.bitField0_ &= -5;
                  } else {
                     this.ensureHostsIsMutable();
                     this.hosts_.addAll(other.hosts_);
                  }

                  this.onChanged();
               }

               this.mergeUnknownFields(other.unknownFields);
               this.onChanged();
               return this;
            }
         }

         public final boolean isInitialized() {
            return true;
         }

         public MongoReaderProto.MongoSplitXattr.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            MongoReaderProto.MongoSplitXattr parsedMessage = null;

            try {
               parsedMessage = (MongoReaderProto.MongoSplitXattr)MongoReaderProto.MongoSplitXattr.PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (InvalidProtocolBufferException var8) {
               parsedMessage = (MongoReaderProto.MongoSplitXattr)var8.getUnfinishedMessage();
               throw var8.unwrapIOException();
            } finally {
               if (parsedMessage != null) {
                  this.mergeFrom(parsedMessage);
               }

            }

            return this;
         }

         public boolean hasMinFilter() {
            return (this.bitField0_ & 1) != 0;
         }

         public String getMinFilter() {
            Object ref = this.minFilter_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.minFilter_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getMinFilterBytes() {
            Object ref = this.minFilter_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.minFilter_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder setMinFilter(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.minFilter_ = value;
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder clearMinFilter() {
            this.bitField0_ &= -2;
            this.minFilter_ = MongoReaderProto.MongoSplitXattr.getDefaultInstance().getMinFilter();
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoSplitXattr.Builder setMinFilterBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.minFilter_ = value;
               this.onChanged();
               return this;
            }
         }

         public boolean hasMaxFilter() {
            return (this.bitField0_ & 2) != 0;
         }

         public String getMaxFilter() {
            Object ref = this.maxFilter_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.maxFilter_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getMaxFilterBytes() {
            Object ref = this.maxFilter_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.maxFilter_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder setMaxFilter(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.maxFilter_ = value;
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder clearMaxFilter() {
            this.bitField0_ &= -3;
            this.maxFilter_ = MongoReaderProto.MongoSplitXattr.getDefaultInstance().getMaxFilter();
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoSplitXattr.Builder setMaxFilterBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.maxFilter_ = value;
               this.onChanged();
               return this;
            }
         }

         private void ensureHostsIsMutable() {
            if ((this.bitField0_ & 4) == 0) {
               this.hosts_ = new LazyStringArrayList(this.hosts_);
               this.bitField0_ |= 4;
            }

         }

         public ProtocolStringList getHostsList() {
            return this.hosts_.getUnmodifiableView();
         }

         public int getHostsCount() {
            return this.hosts_.size();
         }

         public String getHosts(int index) {
            return (String)this.hosts_.get(index);
         }

         public ByteString getHostsBytes(int index) {
            return this.hosts_.getByteString(index);
         }

         public MongoReaderProto.MongoSplitXattr.Builder setHosts(int index, String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureHostsIsMutable();
               this.hosts_.set(index, value);
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder addHosts(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureHostsIsMutable();
               this.hosts_.add(value);
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoSplitXattr.Builder addAllHosts(Iterable<String> values) {
            this.ensureHostsIsMutable();
            com.google.protobuf.AbstractMessageLite.Builder.addAll(values, this.hosts_);
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoSplitXattr.Builder clearHosts() {
            this.hosts_ = LazyStringArrayList.EMPTY;
            this.bitField0_ &= -5;
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoSplitXattr.Builder addHostsBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureHostsIsMutable();
               this.hosts_.add(value);
               this.onChanged();
               return this;
            }
         }

         public final MongoReaderProto.MongoSplitXattr.Builder setUnknownFields(UnknownFieldSet unknownFields) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.setUnknownFields(unknownFields);
         }

         public final MongoReaderProto.MongoSplitXattr.Builder mergeUnknownFields(UnknownFieldSet unknownFields) {
            return (MongoReaderProto.MongoSplitXattr.Builder)super.mergeUnknownFields(unknownFields);
         }

         // $FF: synthetic method
         Builder(Object x0) {
            this();
         }

         // $FF: synthetic method
         Builder(BuilderParent x0, Object x1) {
            this(x0);
         }
      }
   }

   public interface MongoSplitXattrOrBuilder extends MessageOrBuilder {
      boolean hasMinFilter();

      String getMinFilter();

      ByteString getMinFilterBytes();

      boolean hasMaxFilter();

      String getMaxFilter();

      ByteString getMaxFilterBytes();

      List<String> getHostsList();

      int getHostsCount();

      String getHosts(int var1);

      ByteString getHostsBytes(int var1);
   }

   public static final class MongoTableXattr extends GeneratedMessageV3 implements MongoReaderProto.MongoTableXattrOrBuilder {
      private static final long serialVersionUID = 0L;
      private int bitField0_;
      public static final int DATABASE_FIELD_NUMBER = 1;
      private volatile Object database_;
      public static final int COLLECTION_FIELD_NUMBER = 2;
      private volatile Object collection_;
      public static final int TYPE_FIELD_NUMBER = 3;
      private int type_;
      private byte memoizedIsInitialized;
      private static final MongoReaderProto.MongoTableXattr DEFAULT_INSTANCE = new MongoReaderProto.MongoTableXattr();
      /** @deprecated */
      @Deprecated
      public static final Parser<MongoReaderProto.MongoTableXattr> PARSER = new AbstractParser<MongoReaderProto.MongoTableXattr>() {
         public MongoReaderProto.MongoTableXattr parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return new MongoReaderProto.MongoTableXattr(input, extensionRegistry);
         }
      };

      private MongoTableXattr(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
         super(builder);
         this.memoizedIsInitialized = -1;
      }

      private MongoTableXattr() {
         this.memoizedIsInitialized = -1;
         this.database_ = "";
         this.collection_ = "";
         this.type_ = 1;
      }

      protected Object newInstance(UnusedPrivateParameter unused) {
         return new MongoReaderProto.MongoTableXattr();
      }

      public final UnknownFieldSet getUnknownFields() {
         return this.unknownFields;
      }

      private MongoTableXattr(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         this();
         if (extensionRegistry == null) {
            throw new NullPointerException();
         } else {
            int mutable_bitField0_ = false;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();

            try {
               boolean done = false;

               while(!done) {
                  int tag = input.readTag();
                  ByteString bs;
                  switch(tag) {
                  case 0:
                     done = true;
                     break;
                  case 10:
                     bs = input.readBytes();
                     this.bitField0_ |= 1;
                     this.database_ = bs;
                     break;
                  case 18:
                     bs = input.readBytes();
                     this.bitField0_ |= 2;
                     this.collection_ = bs;
                     break;
                  case 24:
                     int rawValue = input.readEnum();
                     MongoReaderProto.CollectionType value = MongoReaderProto.CollectionType.valueOf(rawValue);
                     if (value == null) {
                        unknownFields.mergeVarintField(3, rawValue);
                     } else {
                        this.bitField0_ |= 4;
                        this.type_ = rawValue;
                     }
                     break;
                  default:
                     if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                        done = true;
                     }
                  }
               }
            } catch (InvalidProtocolBufferException var13) {
               throw var13.setUnfinishedMessage(this);
            } catch (IOException var14) {
               throw (new InvalidProtocolBufferException(var14)).setUnfinishedMessage(this);
            } finally {
               this.unknownFields = unknownFields.build();
               this.makeExtensionsImmutable();
            }

         }
      }

      public static final Descriptor getDescriptor() {
         return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
      }

      protected FieldAccessorTable internalGetFieldAccessorTable() {
         return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoReaderProto.MongoTableXattr.class, MongoReaderProto.MongoTableXattr.Builder.class);
      }

      public boolean hasDatabase() {
         return (this.bitField0_ & 1) != 0;
      }

      public String getDatabase() {
         Object ref = this.database_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.database_ = s;
            }

            return s;
         }
      }

      public ByteString getDatabaseBytes() {
         Object ref = this.database_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.database_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public boolean hasCollection() {
         return (this.bitField0_ & 2) != 0;
      }

      public String getCollection() {
         Object ref = this.collection_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.collection_ = s;
            }

            return s;
         }
      }

      public ByteString getCollectionBytes() {
         Object ref = this.collection_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.collection_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public boolean hasType() {
         return (this.bitField0_ & 4) != 0;
      }

      public MongoReaderProto.CollectionType getType() {
         MongoReaderProto.CollectionType result = MongoReaderProto.CollectionType.valueOf(this.type_);
         return result == null ? MongoReaderProto.CollectionType.SINGLE_PARTITION : result;
      }

      public final boolean isInitialized() {
         byte isInitialized = this.memoizedIsInitialized;
         if (isInitialized == 1) {
            return true;
         } else if (isInitialized == 0) {
            return false;
         } else {
            this.memoizedIsInitialized = 1;
            return true;
         }
      }

      public void writeTo(CodedOutputStream output) throws IOException {
         if ((this.bitField0_ & 1) != 0) {
            GeneratedMessageV3.writeString(output, 1, this.database_);
         }

         if ((this.bitField0_ & 2) != 0) {
            GeneratedMessageV3.writeString(output, 2, this.collection_);
         }

         if ((this.bitField0_ & 4) != 0) {
            output.writeEnum(3, this.type_);
         }

         this.unknownFields.writeTo(output);
      }

      public int getSerializedSize() {
         int size = this.memoizedSize;
         if (size != -1) {
            return size;
         } else {
            size = 0;
            if ((this.bitField0_ & 1) != 0) {
               size += GeneratedMessageV3.computeStringSize(1, this.database_);
            }

            if ((this.bitField0_ & 2) != 0) {
               size += GeneratedMessageV3.computeStringSize(2, this.collection_);
            }

            if ((this.bitField0_ & 4) != 0) {
               size += CodedOutputStream.computeEnumSize(3, this.type_);
            }

            size += this.unknownFields.getSerializedSize();
            this.memoizedSize = size;
            return size;
         }
      }

      public boolean equals(Object obj) {
         if (obj == this) {
            return true;
         } else if (!(obj instanceof MongoReaderProto.MongoTableXattr)) {
            return super.equals(obj);
         } else {
            MongoReaderProto.MongoTableXattr other = (MongoReaderProto.MongoTableXattr)obj;
            if (this.hasDatabase() != other.hasDatabase()) {
               return false;
            } else if (this.hasDatabase() && !this.getDatabase().equals(other.getDatabase())) {
               return false;
            } else if (this.hasCollection() != other.hasCollection()) {
               return false;
            } else if (this.hasCollection() && !this.getCollection().equals(other.getCollection())) {
               return false;
            } else if (this.hasType() != other.hasType()) {
               return false;
            } else if (this.hasType() && this.type_ != other.type_) {
               return false;
            } else {
               return this.unknownFields.equals(other.unknownFields);
            }
         }
      }

      public int hashCode() {
         if (this.memoizedHashCode != 0) {
            return this.memoizedHashCode;
         } else {
            int hash = 41;
            int hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasDatabase()) {
               hash = 37 * hash + 1;
               hash = 53 * hash + this.getDatabase().hashCode();
            }

            if (this.hasCollection()) {
               hash = 37 * hash + 2;
               hash = 53 * hash + this.getCollection().hashCode();
            }

            if (this.hasType()) {
               hash = 37 * hash + 3;
               hash = 53 * hash + this.type_;
            }

            hash = 29 * hash + this.unknownFields.hashCode();
            this.memoizedHashCode = hash;
            return hash;
         }
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(ByteBuffer data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(ByteString data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(byte[] data) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (MongoReaderProto.MongoTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(InputStream input) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public static MongoReaderProto.MongoTableXattr parseDelimitedFrom(InputStream input) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoTableXattr parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(CodedInputStream input) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static MongoReaderProto.MongoTableXattr parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (MongoReaderProto.MongoTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public MongoReaderProto.MongoTableXattr.Builder newBuilderForType() {
         return newBuilder();
      }

      public static MongoReaderProto.MongoTableXattr.Builder newBuilder() {
         return DEFAULT_INSTANCE.toBuilder();
      }

      public static MongoReaderProto.MongoTableXattr.Builder newBuilder(MongoReaderProto.MongoTableXattr prototype) {
         return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
      }

      public MongoReaderProto.MongoTableXattr.Builder toBuilder() {
         return this == DEFAULT_INSTANCE ? new MongoReaderProto.MongoTableXattr.Builder() : (new MongoReaderProto.MongoTableXattr.Builder()).mergeFrom(this);
      }

      protected MongoReaderProto.MongoTableXattr.Builder newBuilderForType(BuilderParent parent) {
         MongoReaderProto.MongoTableXattr.Builder builder = new MongoReaderProto.MongoTableXattr.Builder(parent);
         return builder;
      }

      public static MongoReaderProto.MongoTableXattr getDefaultInstance() {
         return DEFAULT_INSTANCE;
      }

      public static Parser<MongoReaderProto.MongoTableXattr> parser() {
         return PARSER;
      }

      public Parser<MongoReaderProto.MongoTableXattr> getParserForType() {
         return PARSER;
      }

      public MongoReaderProto.MongoTableXattr getDefaultInstanceForType() {
         return DEFAULT_INSTANCE;
      }

      // $FF: synthetic method
      MongoTableXattr(com.google.protobuf.GeneratedMessageV3.Builder x0, Object x1) {
         this(x0);
      }

      // $FF: synthetic method
      MongoTableXattr(CodedInputStream x0, ExtensionRegistryLite x1, Object x2) throws InvalidProtocolBufferException {
         this(x0, x1);
      }

      public static final class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<MongoReaderProto.MongoTableXattr.Builder> implements MongoReaderProto.MongoTableXattrOrBuilder {
         private int bitField0_;
         private Object database_;
         private Object collection_;
         private int type_;

         public static final Descriptor getDescriptor() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
         }

         protected FieldAccessorTable internalGetFieldAccessorTable() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoReaderProto.MongoTableXattr.class, MongoReaderProto.MongoTableXattr.Builder.class);
         }

         private Builder() {
            this.database_ = "";
            this.collection_ = "";
            this.type_ = 1;
            this.maybeForceBuilderInitialization();
         }

         private Builder(BuilderParent parent) {
            super(parent);
            this.database_ = "";
            this.collection_ = "";
            this.type_ = 1;
            this.maybeForceBuilderInitialization();
         }

         private void maybeForceBuilderInitialization() {
            if (MongoReaderProto.MongoTableXattr.alwaysUseFieldBuilders) {
            }

         }

         public MongoReaderProto.MongoTableXattr.Builder clear() {
            super.clear();
            this.database_ = "";
            this.bitField0_ &= -2;
            this.collection_ = "";
            this.bitField0_ &= -3;
            this.type_ = 1;
            this.bitField0_ &= -5;
            return this;
         }

         public Descriptor getDescriptorForType() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
         }

         public MongoReaderProto.MongoTableXattr getDefaultInstanceForType() {
            return MongoReaderProto.MongoTableXattr.getDefaultInstance();
         }

         public MongoReaderProto.MongoTableXattr build() {
            MongoReaderProto.MongoTableXattr result = this.buildPartial();
            if (!result.isInitialized()) {
               throw newUninitializedMessageException(result);
            } else {
               return result;
            }
         }

         public MongoReaderProto.MongoTableXattr buildPartial() {
            MongoReaderProto.MongoTableXattr result = new MongoReaderProto.MongoTableXattr(this);
            int from_bitField0_ = this.bitField0_;
            int to_bitField0_ = 0;
            if ((from_bitField0_ & 1) != 0) {
               to_bitField0_ |= 1;
            }

            result.database_ = this.database_;
            if ((from_bitField0_ & 2) != 0) {
               to_bitField0_ |= 2;
            }

            result.collection_ = this.collection_;
            if ((from_bitField0_ & 4) != 0) {
               to_bitField0_ |= 4;
            }

            result.type_ = this.type_;
            result.bitField0_ = to_bitField0_;
            this.onBuilt();
            return result;
         }

         public MongoReaderProto.MongoTableXattr.Builder clone() {
            return (MongoReaderProto.MongoTableXattr.Builder)super.clone();
         }

         public MongoReaderProto.MongoTableXattr.Builder setField(FieldDescriptor field, Object value) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.setField(field, value);
         }

         public MongoReaderProto.MongoTableXattr.Builder clearField(FieldDescriptor field) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.clearField(field);
         }

         public MongoReaderProto.MongoTableXattr.Builder clearOneof(OneofDescriptor oneof) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.clearOneof(oneof);
         }

         public MongoReaderProto.MongoTableXattr.Builder setRepeatedField(FieldDescriptor field, int index, Object value) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.setRepeatedField(field, index, value);
         }

         public MongoReaderProto.MongoTableXattr.Builder addRepeatedField(FieldDescriptor field, Object value) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.addRepeatedField(field, value);
         }

         public MongoReaderProto.MongoTableXattr.Builder mergeFrom(Message other) {
            if (other instanceof MongoReaderProto.MongoTableXattr) {
               return this.mergeFrom((MongoReaderProto.MongoTableXattr)other);
            } else {
               super.mergeFrom(other);
               return this;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder mergeFrom(MongoReaderProto.MongoTableXattr other) {
            if (other == MongoReaderProto.MongoTableXattr.getDefaultInstance()) {
               return this;
            } else {
               if (other.hasDatabase()) {
                  this.bitField0_ |= 1;
                  this.database_ = other.database_;
                  this.onChanged();
               }

               if (other.hasCollection()) {
                  this.bitField0_ |= 2;
                  this.collection_ = other.collection_;
                  this.onChanged();
               }

               if (other.hasType()) {
                  this.setType(other.getType());
               }

               this.mergeUnknownFields(other.unknownFields);
               this.onChanged();
               return this;
            }
         }

         public final boolean isInitialized() {
            return true;
         }

         public MongoReaderProto.MongoTableXattr.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            MongoReaderProto.MongoTableXattr parsedMessage = null;

            try {
               parsedMessage = (MongoReaderProto.MongoTableXattr)MongoReaderProto.MongoTableXattr.PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (InvalidProtocolBufferException var8) {
               parsedMessage = (MongoReaderProto.MongoTableXattr)var8.getUnfinishedMessage();
               throw var8.unwrapIOException();
            } finally {
               if (parsedMessage != null) {
                  this.mergeFrom(parsedMessage);
               }

            }

            return this;
         }

         public boolean hasDatabase() {
            return (this.bitField0_ & 1) != 0;
         }

         public String getDatabase() {
            Object ref = this.database_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.database_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getDatabaseBytes() {
            Object ref = this.database_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.database_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder setDatabase(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.database_ = value;
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder clearDatabase() {
            this.bitField0_ &= -2;
            this.database_ = MongoReaderProto.MongoTableXattr.getDefaultInstance().getDatabase();
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoTableXattr.Builder setDatabaseBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.database_ = value;
               this.onChanged();
               return this;
            }
         }

         public boolean hasCollection() {
            return (this.bitField0_ & 2) != 0;
         }

         public String getCollection() {
            Object ref = this.collection_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.collection_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getCollectionBytes() {
            Object ref = this.collection_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.collection_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder setCollection(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.collection_ = value;
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder clearCollection() {
            this.bitField0_ &= -3;
            this.collection_ = MongoReaderProto.MongoTableXattr.getDefaultInstance().getCollection();
            this.onChanged();
            return this;
         }

         public MongoReaderProto.MongoTableXattr.Builder setCollectionBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.collection_ = value;
               this.onChanged();
               return this;
            }
         }

         public boolean hasType() {
            return (this.bitField0_ & 4) != 0;
         }

         public MongoReaderProto.CollectionType getType() {
            MongoReaderProto.CollectionType result = MongoReaderProto.CollectionType.valueOf(this.type_);
            return result == null ? MongoReaderProto.CollectionType.SINGLE_PARTITION : result;
         }

         public MongoReaderProto.MongoTableXattr.Builder setType(MongoReaderProto.CollectionType value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 4;
               this.type_ = value.getNumber();
               this.onChanged();
               return this;
            }
         }

         public MongoReaderProto.MongoTableXattr.Builder clearType() {
            this.bitField0_ &= -5;
            this.type_ = 1;
            this.onChanged();
            return this;
         }

         public final MongoReaderProto.MongoTableXattr.Builder setUnknownFields(UnknownFieldSet unknownFields) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.setUnknownFields(unknownFields);
         }

         public final MongoReaderProto.MongoTableXattr.Builder mergeUnknownFields(UnknownFieldSet unknownFields) {
            return (MongoReaderProto.MongoTableXattr.Builder)super.mergeUnknownFields(unknownFields);
         }

         // $FF: synthetic method
         Builder(Object x0) {
            this();
         }

         // $FF: synthetic method
         Builder(BuilderParent x0, Object x1) {
            this(x0);
         }
      }
   }

   public interface MongoTableXattrOrBuilder extends MessageOrBuilder {
      boolean hasDatabase();

      String getDatabase();

      ByteString getDatabaseBytes();

      boolean hasCollection();

      String getCollection();

      ByteString getCollectionBytes();

      boolean hasType();

      MongoReaderProto.CollectionType getType();
   }

   public static enum CollectionType implements ProtocolMessageEnum {
      SINGLE_PARTITION(1),
      SUB_PARTITIONED(2),
      NODE_PARTITION(3),
      RANGE_PARTITION(4);

      public static final int SINGLE_PARTITION_VALUE = 1;
      public static final int SUB_PARTITIONED_VALUE = 2;
      public static final int NODE_PARTITION_VALUE = 3;
      public static final int RANGE_PARTITION_VALUE = 4;
      private static final EnumLiteMap<MongoReaderProto.CollectionType> internalValueMap = new EnumLiteMap<MongoReaderProto.CollectionType>() {
         public MongoReaderProto.CollectionType findValueByNumber(int number) {
            return MongoReaderProto.CollectionType.forNumber(number);
         }
      };
      private static final MongoReaderProto.CollectionType[] VALUES = values();
      private final int value;

      public final int getNumber() {
         return this.value;
      }

      /** @deprecated */
      @Deprecated
      public static MongoReaderProto.CollectionType valueOf(int value) {
         return forNumber(value);
      }

      public static MongoReaderProto.CollectionType forNumber(int value) {
         switch(value) {
         case 1:
            return SINGLE_PARTITION;
         case 2:
            return SUB_PARTITIONED;
         case 3:
            return NODE_PARTITION;
         case 4:
            return RANGE_PARTITION;
         default:
            return null;
         }
      }

      public static EnumLiteMap<MongoReaderProto.CollectionType> internalGetValueMap() {
         return internalValueMap;
      }

      public final EnumValueDescriptor getValueDescriptor() {
         return (EnumValueDescriptor)getDescriptor().getValues().get(this.ordinal());
      }

      public final EnumDescriptor getDescriptorForType() {
         return getDescriptor();
      }

      public static final EnumDescriptor getDescriptor() {
         return (EnumDescriptor)MongoReaderProto.getDescriptor().getEnumTypes().get(0);
      }

      public static MongoReaderProto.CollectionType valueOf(EnumValueDescriptor desc) {
         if (desc.getType() != getDescriptor()) {
            throw new IllegalArgumentException("EnumValueDescriptor is not for this type.");
         } else {
            return VALUES[desc.getIndex()];
         }
      }

      private CollectionType(int value) {
         this.value = value;
      }
   }
}
