
package avrofdb;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static com.foundationdb.tuple.ByteArrayUtil.decodeInt;
import static com.foundationdb.tuple.ByteArrayUtil.encodeInt;
import static com.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

public class AvroDatabase<V extends SpecificRecord> {
  private static final Logger log = Logger.getLogger("AvroDB");

  private final Database database;
  private final Schema readerSchema;
  private final DirectorySubspace schemaRoot;
  private final DirectorySubspace tableRoot;
  private final DirectorySubspace avrofdb;
  private final DirectorySubspace indexesRoot;
  private final Map<String, Schema.Field> fieldByName = new HashMap<>();
  private final Map<Schema.Field, DirectorySubspace> subspaceByField = new HashMap<>();
  private final Map<Schema.Field, Index> indexByField = new HashMap<>();


  private final BiMap<Schema, Long> schemaMap = HashBiMap.create();
  private final DecoderFactory df;
  private final EncoderFactory ef;
  private final ThreadLocal<Transaction> currentTx = new ThreadLocal<>();
  private final Tuple schemaId;
  private final DirectorySubspace tableIndexesRoot;

  public <T> T runTx(Supplier<T> supplier) {
    return run(tx -> supplier.get());
  }

  private <T> T run(Function<Transaction, T> function) {
    Transaction tx = currentTx.get();
    if (tx == null) {
      return database.run(new Function<Transaction, T>() {
        @Override
        public T apply(Transaction newTx) {
          currentTx.set(newTx);
          try {
            return function.apply(newTx);
          } finally {
            currentTx.remove();
          }
        }
      });
    } else {
      return function.apply(tx);
    }
  }

  private <T> Future<T> runAsync(Function<Transaction, Future<T>> function) {
    Transaction tx = currentTx.get();
    if (tx == null) {
      return database.runAsync(new Function<Transaction, Future<T>>() {
        @Override
        public Future<T> apply(Transaction newTx) {
          currentTx.set(newTx);
          try {
            return database.run(function);
          } finally {
            currentTx.remove();
          }
        }
      });
    } else {
      return function.apply(tx);
    }
  }

  public AvroDatabase(Database database, String tableName, Schema readerSchema, Indexes indexes, boolean reindex) {
    this.database = database;
    this.readerSchema = readerSchema;
    DirectoryLayer dl = DirectoryLayer.getDefault();
    avrofdb = dl.createOrOpen(database, asList("avrofdb")).get();
    schemaRoot = avrofdb.createOrOpen(database, asList("schemas")).get();
    tableRoot = avrofdb.createOrOpen(database, asList("tables", tableName)).get();
    indexesRoot = avrofdb.createOrOpen(database, asList("indexes")).get();
    tableIndexesRoot = indexesRoot.createOrOpen(database, asList(tableName)).get();

    Indexes currentIndexes = run(new Function<Transaction, Indexes>() {
      @Override
      public Indexes apply(Transaction transaction) {
        byte[] bytes = transaction.get(indexesRoot.pack(tableName)).get();
        if (bytes == null) {
          return null;
        }
        BinaryDecoder be = df.binaryDecoder(bytes, null);
        SpecificDatumReader<Indexes> reader = new SpecificDatumReader<>(Indexes.getClassSchema());
        try {
          return reader.read(null, be);
        } catch (IOException e) {
          throw new AssertionError("Could not read indexes for " + tableName, e);
        }
      }
    });

    schemaMap.putAll(run(tx -> {
      // Load all the schemas from the database
      Map<Schema, Long> map = stream(tx.getRange(schemaRoot.range()).spliterator(), false)
              .collect(toMap(kv -> new Schema.Parser().parse(new String(kv.getValue(), Charsets.UTF_8)),
                      kv -> schemaRoot.unpack(kv.getKey()).getLong(0)));
      // If this schema isn't present, add it to the schemas
      if (!map.containsKey(readerSchema)) {
        byte[] key = avrofdb.pack("sequence");
        tx.mutate(MutationType.ADD, key, encodeInt(1));
        long id = decodeInt(tx.get(key).get());
        tx.set(schemaRoot.pack(id), readerSchema.toString().getBytes(Charsets.UTF_8));
        map.put(readerSchema, id);
      }
      return map;
    }));

    df = new DecoderFactory();
    ef = new EncoderFactory();

    schemaId = Tuple.from(schemaMap.get(readerSchema));

    // Do the indexes need to be regenerated to continue?
    if (!Objects.equals(currentIndexes, indexes)) {
      // If there were no indexes before, new indexes are always ok
      if (reindex || currentIndexes == null) {
        log.info("Reindexing is not yet implemented");
      } else {
        throw new IllegalArgumentException("Current database indexes: " + currentIndexes + " do not match new indexes: " + indexes);
      }
    }

    // Build up our index lookups
    for (Index index : indexes.getIndexes()) {
      String fieldName = index.getFieldName();
      Schema.Field field = readerSchema.getField(fieldName);
      if (field == null) {
        throw new IllegalArgumentException("No such field named: " + fieldName);
      }
      Schema.Type type = field.schema().getType();
      switch (type) {
        case STRING:
        case BYTES:
        case INT:
        case LONG:
          // OK
          break;
        default:
          throw new IllegalArgumentException("Cannot index type: " + type);
      }
      fieldByName.put(fieldName, field);
      indexByField.put(field, index);
      subspaceByField.put(field, tableIndexesRoot.createOrOpen(database, asList(fieldName)).get());
    }
  }

  public Future<V> get(Tuple tuple) {
    byte[] key = tableRoot.pack(tuple);
    return runAsync(tx -> tx.get(key).map(new Function<byte[], V>() {
      @Override
      public V apply(byte[] bytes) {
        if (bytes == null) {
          return null;
        } else {
          Tuple value = Tuple.fromBytes(bytes);
          long id = value.getLong(0);
          Schema writerSchema = schemaMap.inverse().get(id);
          if (writerSchema == null) {
            byte[] schemaBytes = tx.get(schemaRoot.pack(Tuple.from(id))).get();
            if (schemaBytes == null) {
              throw new AssertionError("Schema with id " + id + " not found in schemas");
            }
            schemaMap.put(writerSchema = new Schema.Parser().parse(new String(schemaBytes, Charsets.UTF_8)), id);
          }
          byte[] avroBytes = value.getBytes(1);
          BinaryDecoder bd = df.binaryDecoder(avroBytes, null);
          SpecificDatumReader<V> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
          try {
            return reader.read(null, bd);
          } catch (IOException e) {
            throw new AssertionError("Failed to decode avro object: " + printable(key) + " -> " + printable(avroBytes));
          }
        }
      }
    }
    ));
  }

  public void set(Tuple tuple, V object) {
    byte[] key = tableRoot.pack(tuple);
    run(tx -> {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder be = ef.binaryEncoder(baos, null);
      SpecificDatumWriter<V> writer = new SpecificDatumWriter<>(readerSchema);
      try {
        writer.write(object, be);
        be.flush();
      } catch (IOException e) {
        throw new AssertionError("Failed to write data: " + object, e);
      }
      byte[] localKey = tuple.pack();
      deindex(tx, localKey, get(tuple).get());
      index(tx, localKey, object);
      tx.set(key, schemaId.add(baos.toByteArray()).pack());
      return null;
    });
  }

  public void clear(Tuple tuple) {
    byte[] key = tableRoot.pack(tuple);
    run(tx -> {
      deindex(tx, tuple.pack(), get(tuple).get());
      tx.clear(key);
      return null;
    });
  }

  public List<V> search(String fieldName, Object o) {
    Schema.Field field = fieldByName.get(fieldName);
    DirectorySubspace subspace = subspaceByField.get(field);
    Index index = indexByField.get(field);
    if (index.getCaseInSensitive()) {
      o = o.toString().toLowerCase();
    }
    final Object finalO = o;
    return run(new Function<Transaction, List<V>>() {
      @Override
      public List<V> apply(Transaction tx) {
        List<V> list = new ArrayList<>();
        for (KeyValue keyValue : tx.getRange(Range.startsWith(subspace.pack(finalO)))) {
          Tuple unpack = subspace.unpack(keyValue.getKey());
          Tuple tuple = Tuple.fromBytes(unpack.getBytes(1));
          list.add(get(tuple).get());
        }
        return list;
      }
    });
  }

  private void deindex(Transaction tx, byte[] key, SpecificRecord object) {
    if (object == null) return;
    for (Map.Entry<Schema.Field, Index> entry : indexByField.entrySet()) {
      Schema.Field field = entry.getKey();
      Index index = entry.getValue();
      Object o = getObject(object, field, index);
      DirectorySubspace subspace = subspaceByField.get(field);
      tx.clear(subspace.pack(Tuple.from(o, key)));
    }
  }

  private void index(Transaction tx, byte[] key, SpecificRecord object) {
    for (Map.Entry<Schema.Field, Index> entry : indexByField.entrySet()) {
      Schema.Field field = entry.getKey();
      Index index = entry.getValue();
      Object o = getObject(object, field, index);
      DirectorySubspace subspace = subspaceByField.get(field);
      if (index.getUnique()) {
        if (tx.getRange(Range.startsWith(subspace.pack(o))).iterator().hasNext()) {
          throw new IllegalArgumentException("Violation of uniqueness constraint: " + index + ": " + object);
        }
      }
      tx.set(subspace.pack(Tuple.from(o, key)), new byte[0]);
    }
  }

  private Object getObject(SpecificRecord object, Schema.Field field, Index index) {
    Object o = object.get(field.pos());
    if (index.getCaseInSensitive()) {
      o = o.toString().toLowerCase();
    }
    return o;
  }
}
