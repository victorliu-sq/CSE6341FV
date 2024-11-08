package src;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static src.Serializability.checkSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    void executeTransactionsParallelTest() throws InterruptedException {
        int numRows = 3;
        int numThreads = 3;

        List<Transaction>[] batchLists = new LinkedList[numThreads];
        List<Transaction> allTXNs = new LinkedList<>();
        List<Transaction> allTXNsCopy = new LinkedList<>();

        Transaction t1 = new Transaction(0);
        t1.addOperation(new Operation(0, 1, 0));
        t1.addOperation(new Operation(0, 2, 0));
        t1.addOperation(new Operation(1, 1, 0));
        Transaction t1Copy = new Transaction(t1);

        Transaction t2 = new Transaction(1);
        t2.addOperation(new Operation(0, 1, 0));
        t2.addOperation(new Operation(0, 2, 0));
        t2.addOperation(new Operation(1, 1, 2));
        Transaction t2Copy = new Transaction(t2);

        Transaction t3 = new Transaction(2);
        t3.addOperation(new Operation(1, 0, 3));
        t3.addOperation(new Operation(0, 1, 0));
        t3.addOperation(new Operation(1, 2, 3));
        Transaction t3Copy = new Transaction(t3);

        List<Transaction> batch1 = new LinkedList<>();
        batch1.add(t1);
        batchLists[0] = batch1;

        List<Transaction> batch2 = new LinkedList<>();
        batch2.add(t2);
        batchLists[1] = batch2;

        List<Transaction> batch3 = new LinkedList<>();
        batch3.add(t3);
        batchLists[2] = batch3;

        allTXNs.add(t1);
        allTXNs.add(t2);
        allTXNs.add(t3);

        allTXNsCopy.add(t1Copy);
        allTXNsCopy.add(t2Copy);
        allTXNsCopy.add(t3Copy);

        PipelineDB db = new PipelineDB(numRows, numThreads);

        ExecutorService threadpool = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            int index = i;
            threadpool.submit(() -> db.executeTransactions(batchLists[index]));
        }

        threadpool.shutdown();
        while (!threadpool.isTerminated()) {
            Thread.sleep(100);
        }

        Assertions.assertEquals(true, Serializability.checkSerializable(numRows, allTXNsCopy, allTXNs, db.getRows()));
    }
}