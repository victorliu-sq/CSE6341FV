package src;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class PipelineDB {
    private int numRows;
    private int numThreads;

    private Row[] rows;

    private LinkedBlockingQueue<Transaction>[] pipeline;
    private Thread[] threads;
//    private CountDownLatch latch;

    private HashMap<Integer, CountDownLatch> TXNID2Latch;

    public Row[] getRows() {
        return rows;
    }

    /**
     * @param numRows
     * @param numThreads
     */
    public PipelineDB(int numRows, int numThreads) {
        this.numRows = numRows;
        this.numThreads = numThreads;

        rows = new Row[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = new Row(-1);
        }

        pipeline = new LinkedBlockingQueue[this.numThreads];
        for (int i = 0; i < numThreads; i++) {
            pipeline[i] = new LinkedBlockingQueue<>();
        }

        threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            int index = i;
            int interval = numRows / numThreads;
            int low = index * interval;
            int high = index == numThreads - 1 ? numRows - 1 : (index + 1) * interval - 1;
            System.out.println("Thread " + index + " is responsible for rows between " + low + " and " + high);
            threads[index] = new Thread(() -> runPipeline(index, low, high));
            threads[index].start();
        }

        // Init Mapping from TXNID to Latch
        TXNID2Latch = new HashMap<>();

        System.out.println("Pipeline has been initialized");
        System.out.println();
    }

    /**
     * @param index
     */
    private void runPipeline(int index, int low, int high) {
        while (true) {
            try {
                Transaction txn = pipeline[index].take();
                for (Operation op : txn.getOperations()) {
                    int rid = op.getRowNumber();
                    if (rid >= low && rid <= high) {
                        if (op.getType() == 0) {
                            op.setValue(rows[rid].getValue());
                            System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " reads row " + rid + " as " + op.getValue());
                        } else {
                            rows[rid].setValue(op.getValue());
                            System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " sets row " + rid + " to " + op.getValue());
                        }
                    }
                }

                if (index != pipeline.length - 1) {
                    pipeline[index + 1].put(txn);
                    System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " is added to the next slot [" + index + "]");
                } else {
                    // countDown the latch of corresponding TXN
                    TXNID2Latch.get(txn.getID()).countDown();
                    System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " has completed");
                }

            } catch (Exception e) {
                System.out.println("Error in thread " + index + ": " + e.toString());
            }
        }

    }

    public void executeTransactions(List<Transaction> transactions) {
        System.out.println("Start to execute transactions:");

        for (Transaction t : transactions) {
            try {
                // Generate a random delay between 100 and 200 milliseconds
                int delay = ThreadLocalRandom.current().nextInt(100, 551);
                System.out.println("Thread sleeping for " + delay + " ms");

                // Put the thread to sleep for the generated delay
                Thread.sleep(delay);
                System.out.println("Thread awake after " + delay + " ms");

                CountDownLatch latch = new CountDownLatch(1);
                TXNID2Latch.put(t.getID(), latch);


                // Put the transaction into pipeline
                pipeline[0].put(t);
                System.out.println("Transaction " + t.getID() + " is added to pipeline");
            } catch (Exception e) {
                System.out.println("Error in transaction " + t.toString());
            }
        }

        try {
            for (Transaction t : transactions) {
                TXNID2Latch.get(t.getID()).await();
            }
            System.out.println("All TXNs have completed");
            System.out.println();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            System.out.println("Execution interrupted: " + e.toString());
        }
    }
}
