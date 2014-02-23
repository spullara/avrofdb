package avrofdb;

import com.foundationdb.Transaction;


public class AvroTransaction {

  private final Transaction transaction;

  protected AvroTransaction(Transaction transaction) {
    this.transaction = transaction;
  }
}
