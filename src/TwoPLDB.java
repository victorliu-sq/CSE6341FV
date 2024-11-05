package src;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TwoPLDB {
    private int numRows;

    private Row rows[];

    private Map<Integer, ReentrantReadWriteLock> locks;

    public TwoPLDB() {
        numRows = 100;
        rows = new Row[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = new Row(i);
        }

        locks = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            locks.put(i, new ReentrantReadWriteLock());
        }
    }

    public TwoPLDB(int numRows) {
        this.numRows = numRows;
        rows = new Row[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = new Row(i);
        }

        locks = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            locks.put(i, new ReentrantReadWriteLock());
        }
    }

    public Row[] getRows() {
        return rows;
    }

    // Method to execute a single transaction
    private void executeTransaction(Transaction txn) {
        boolean success = false;

        while (!success) {
            List<Integer> ridsR = new ArrayList<>();
            List<Integer> ridsW = new ArrayList<>();
            Map<Integer, Integer> oldValues = new HashMap<>();
            boolean aborted = false;

            // Growing Phase
            for (Operation op : txn.getOperations()) {
                Integer rid = op.getRowNumber();
                if (op.getType() == 0) { // Read operation
                    if (locks.get(rid).readLock().tryLock()) { // Acquire read lock
                        ridsR.add(rid);
                        op.setValue(rows[rid].getValue());
                        System.out.println("Transaction " + txn.getID() + " reads row " + rid + " = " + op.getValue());
                    } else {
                        aborted = true;
                        break;
                    }
                } else { // Write operation
                    if (locks.get(rid).writeLock().tryLock()) { // Acquire write lock
                        ridsW.add(rid);
                        oldValues.put(rid, rows[rid].getValue()); // Save old value for rollback
                        rows[rid].setValue(op.getValue());
                        System.out.println("Transaction " + txn.getID() + " sets row " + rid + " = " + op.getValue());
                    } else {
                        aborted = true;
                        break;
                    }
                }
            }

            // If aborted, perform rollback and retry
            if (aborted) {
                System.out.println("Transaction " + txn.getID() + " aborted. Retrying...");
                for (Map.Entry<Integer, Integer> entry : oldValues.entrySet()) {
                    rows[entry.getKey()].setValue(entry.getValue());
                }
                // Release locks to allow retry
                for (Integer rid : ridsR) {
                    locks.get(rid).readLock().unlock();
                }
                for (Integer rid : ridsW) {
                    locks.get(rid).writeLock().unlock();
                }
                continue; // Retry the transaction
            }

            // Releasing Phase if transaction is successful
            for (Integer rid : ridsR) {
                locks.get(rid).readLock().unlock();
            }
            for (Integer rid : ridsW) {
                locks.get(rid).writeLock().unlock();
            }

            success = true; // Transaction completed successfully
        }
    }

    // Method to execute all transactions in parallel
    public void executeTransactions(List<Transaction> transactions) {
        List<Thread> threads = new ArrayList<>();

        // Create and start a thread for each transaction
        for (Transaction txn : transactions) {
            Thread thread = new Thread(() -> executeTransaction(txn));
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}