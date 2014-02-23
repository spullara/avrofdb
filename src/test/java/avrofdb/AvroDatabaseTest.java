package avrofdb;

import avrofdb.test.TestRecord;
import com.foundationdb.FDB;
import com.foundationdb.tuple.Tuple;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class AvroDatabaseTest {

  private static AvroDatabase<TestRecord> adb;

  @BeforeClass
  public static void setup() {
    Indexes index = Indexes.newBuilder().setIndexes(Arrays.asList(Index.newBuilder().setCaseInSensitive(true).setFieldName("host").setUnique(true).build())).build();
    adb = new AvroDatabase<>(FDB.selectAPIVersion(200).open(), "test", TestRecord.getClassSchema(), index, true);
  }

  @Test
  public void testSimpleSetGet() {
    Tuple tuple = Tuple.from("testkey");
    adb.clear(tuple);
    TestRecord testRecord = adb.get(tuple).get();
    assertEquals(null, testRecord);
    testRecord = TestRecord.newBuilder().setHost("localhost").setPort(3306).build();
    adb.set(tuple, testRecord);
    assertEquals(testRecord, adb.get(tuple).get());
  }

  @Test
  public void testTransaction() {
    Tuple tuple = Tuple.from("testkey");
    assertEquals("localhost", adb.runTx(() -> {
      adb.clear(tuple);
      TestRecord testRecord = adb.get(tuple).get();
      assertEquals(null, testRecord);
      testRecord = TestRecord.newBuilder().setHost("localhost").setPort(3306).build();
      adb.set(tuple, testRecord);
      assertEquals(testRecord, adb.get(tuple).get());
      return testRecord.getHost();
    }));
  }

  @Test
  public void testContention() {
    Tuple tuple = Tuple.from("testcontention");
    adb.clear(tuple);
    ExecutorService es = Executors.newCachedThreadPool();
    AtomicInteger runs = new AtomicInteger();
    Semaphore semaphore = new Semaphore(100);
    for (int i = 0; i < 100; i++) {
      semaphore.acquireUninterruptibly();
      es.submit(() -> {
        adb.runTx(() -> {
          runs.incrementAndGet();
          TestRecord testRecord = adb.get(tuple).get();
          if (testRecord == null) {
            testRecord = TestRecord.newBuilder().setHost("localhost").setPort(0).build();
          }
          testRecord.setPort(testRecord.getPort() + 1);
          adb.set(tuple, testRecord);
          return null;
        });
        semaphore.release();
      });
    }
    semaphore.acquireUninterruptibly(100);
    assertEquals(100, (int) adb.get(tuple).get().getPort());
    System.out.println(runs);
  }

  @Test
  public void testIndex() {
    Tuple tuple = Tuple.from("testindex");
    adb.clear(tuple);
    TestRecord testRecord = adb.get(tuple).get();
    assertEquals(null, testRecord);
    testRecord = TestRecord.newBuilder().setHost("indexedhost").setPort(3306).build();
    adb.set(tuple, testRecord);
    assertEquals(testRecord, adb.get(tuple).get());
    assertEquals(testRecord, adb.search("host", "indexedhost").get(0));
  }
}
