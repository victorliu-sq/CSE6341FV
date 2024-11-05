package src;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;

class TwoPLDBTest {

    @Test
    void executeTransactions2Test() {
        int numRows = 5;
        Transaction t1 = new Transaction(0);
        t1.addOperation(new Operation(0, 3, 0));
        t1.addOperation(new Operation(1, 4, 5));

        Transaction t2 = new Transaction(1);
        t2.addOperation(new Operation(1, 4, 99));
        t2.addOperation(new Operation(0, 3, 0));

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        LinkedList<Transaction> batchCopy = new LinkedList<Transaction>(batch);

        TwoPLDB db = new TwoPLDB(numRows);
        db.executeTransactions(batch);

        Assertions.assertEquals(true, Serializability.checkSerializable(numRows, batchCopy, batch, db.getRows()));
    }
}