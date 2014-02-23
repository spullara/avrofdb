
package avrofdb;

import com.foundationdb.Database;
import com.foundationdb.MutationType;
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
import java.util.Map;
import java.util.function.Supplier;

import static com.foundationdb.tuple.ByteArrayUtil.decodeInt;
import static com.foundationdb.tuple.ByteArrayUtil.encodeInt;
import static com.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

public class AvroDatabase<V extends SpecificRecord> {
  private final Database database;
  private final Schema readerSchema;
  private final DirectorySubspace schemas;
  private final DirectorySubspace table;
  private final DirectorySubspace avrofdb;

  private final BiMap<Schema, Long> schemaMap = HashBiMap.create();
  private final DecoderFactory df;
  private final EncoderFactory ef;
  private final ThreadLocal<Transaction> currentTx = new ThreadLocal<>();
  private final Tuple schemaId;

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

  public AvroDatabase(Database database, String tableName, Schema readerSchema) {
    this.database = database;
    this.readerSchema = readerSchema;
    DirectoryLayer dl = DirectoryLayer.getDefault();
    avrofdb = dl.createOrOpen(database, asList("avrofdb")).get();
    schemas = avrofdb.createOrOpen(database, asList("schemas")).get();
    table = avrofdb.createOrOpen(database, asList("tables", tableName)).get();

    schemaMap.putAll(run(tx -> {
      // Load all the schemas from the database
      Map<Schema, Long> map = stream(tx.getRange(schemas.range()).spliterator(), false)
              .collect(toMap(kv -> new Schema.Parser().parse(new String(kv.getValue(), Charsets.UTF_8)),
                      kv -> schemas.unpack(kv.getKey()).getLong(0)));
      // If this schema isn't present, add it to the schemas
      if (!map.containsKey(readerSchema)) {
        byte[] key = avrofdb.pack("sequence");
        tx.mutate(MutationType.ADD, key, encodeInt(1));
        long id = decodeInt(tx.get(key).get());
        tx.set(schemas.pack(id), readerSchema.toString().getBytes(Charsets.UTF_8));
        map.put(readerSchema, id);
      }
      return map;
    }));

    df = new DecoderFactory();
    ef = new EncoderFactory();

    schemaId = Tuple.from(schemaMap.get(readerSchema));
  }

  public Future<V> get(Tuple tuple) {
    byte[] key = tuple.pack();
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
            byte[] schemaBytes = tx.get(schemas.pack(Tuple.from(id))).get();
            if (schemaBytes == null) {
              throw new AssertionError("Schema with id " + id + " not found in schemas");
            }
            schemaMap.put(writerSchema = new Schema.Parser().parse(new String(schemaBytes, Charsets.UTF_8)), id);
          }
          byte[] avroBytes = value.getBytes(1);
          BinaryDecoder bd = df.binaryDecoder(avroBytes, null);
          SpecificDatumReader<V> reader = new SpecificDatumReader<V>(writerSchema, readerSchema);
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
    byte[] key = tuple.pack();
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
      tx.set(key, schemaId.add(baos.toByteArray()).pack());
      return null;
    });
  }

  public void clear(Tuple tuple) {
    byte[] key = tuple.pack();
    run(tx -> {
      tx.clear(key);
      return null;
    });
  }
}
