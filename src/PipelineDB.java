package src;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class PipelineDB {
    private int numRows;
    private int numThreads;

    private Row[] rows;

    private LinkedBlockingQueue<Transaction>[] pipeline;
    private Thread[] threads;
    private CountDownLatch latch;

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
            rows[i] = new Row(i);
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
                            System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " reads row " + rid + " = " + op.getValue());
                        } else {
                            rows[rid].setValue(op.getValue());
                            System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " sets row " + rid + " = " + op.getValue());
                        }
                    }
                }

                if (index != pipeline.length - 1) {
                    pipeline[index + 1].put(txn);
                    System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " is added to the next slot [" + index + "]");
                } else {
                    latch.countDown();
                    System.out.println("Pipeline slot[" + index + "]: Transaction " + txn.getID() + " has completed");
                }

            } catch (Exception e) {
                System.out.println("Error in thread " + index + ": " + e.toString());
            }
        }

    }

    public void executeTransactions(List<Transaction> transactions) {
        System.out.println("Start to execute transactions:");
        latch = new CountDownLatch(transactions.size());

        for (Transaction t : transactions) {
            try {
                pipeline[0].put(t);
                System.out.println("Transaction " + t.getID() + " is added to pipeline");
            } catch (Exception e) {
                System.out.println("Error in transaction " + t.toString());
            }
        }

        try {
            latch.await(); // Wait until all transactions have been processed
            System.out.println("All TXNs have completed");
            System.out.println();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            System.out.println("Execution interrupted: " + e.toString());
        }
    }
}
