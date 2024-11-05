package src;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static src.Serializability.checkSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;

import src.Permutation;
import src.Serializability;

class PipelineDBTest {
    @Test
    void PipelineDBTest1() {
        final int numRows = 5;
        Transaction t1 = new Transaction(0);
        t1.addOperation(new Operation(0, 3, 0));
        t1.addOperation(new Operation(1, 4, 5));

        Transaction t2 = new Transaction(1);
        t2.addOperation(new Operation(1, 3, 99));
        t2.addOperation(new Operation(0, 4, 0));

        List<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);

        List<Transaction> batchCopy = new LinkedList<Transaction>(batch);

        PipelineDB db = new PipelineDB(numRows, 3);
        db.executeTransactions(batch);

        Assertions.assertEquals(true, Serializability.checkSerializable(numRows, batchCopy, batch, db.getRows()));
    }
}