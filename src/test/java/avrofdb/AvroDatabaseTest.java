package avrofdb;

import avrofdb.test.TestRecord;
import com.foundationdb.FDB;
import com.foundationdb.tuple.Tuple;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

public class AvroDatabaseTest {

  private static AvroDatabase<TestRecord> adb;

  @BeforeClass
  public static void setup() {
    adb = new AvroDatabase<>(FDB.selectAPIVersion(200).open(), "test", TestRecord.getClassSchema());
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
    Semaphore semaphore = new Semaphore(100);
    for (int i = 0; i < 100; i++) {
      semaphore.acquireUninterruptibly();
      es.submit(() -> {
        adb.runTx(() -> {
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
  }

}
