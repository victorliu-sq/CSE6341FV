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
            rows[i] = new Row(-1);
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
            rows[i] = new Row(-1);
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

        // map rowID to access type (0: read, 1:write)
        Map<Integer, Integer> rid2LockType = new HashMap<>();
        for (Operation op : txn.getOperations()) {
            Integer rid = op.getRowNumber();
            if (op.getType() == 0 && !rid2LockType.containsKey(rid)) {
                rid2LockType.put(rid, op.getType());
            } else if (op.getType() == 1) {
                rid2LockType.put(rid, op.getType());
            }
        }

        // acquire locks for all rows

        while (!success) {
            Map<Integer, Integer> oldValues = new HashMap<>();
            boolean aborted = false;

            // map rowID to locked or not to avoid duplicate locking
            Map<Integer, Boolean> rid2Locked = new HashMap<>();

            // Growing Phase
            for (Operation op : txn.getOperations()) {
                Integer rid = op.getRowNumber();
                if (!rid2Locked.containsKey(rid)) {
                    if (rid2LockType.get(rid) == 0) {
                        if (!locks.get(rid).readLock().tryLock()) {
                            aborted = true;
                        } else {
                            rid2Locked.put(rid, true);
                        }
                    } else {
                        if (!locks.get(rid).writeLock().tryLock()) {
                            aborted = true;
                        } else {
                            rid2Locked.put(rid, true);
                        }
                    }
                }

                if (op.getType() == 0) {
                    op.setValue(rows[rid].getValue());
                    System.out.println("Transaction " + txn.getID() + " reads row " + rid + " as " + op.getValue());
                } else {
                    oldValues.put(rid, rows[rid].getValue()); // Save old value for rollback
                    rows[rid].setValue(op.getValue());
                    System.out.println("Transaction " + txn.getID() + " sets row " + rid + " to " + op.getValue());
                }
            }

            // If aborted, perform rollback and retry
            if (aborted) {
                // cascading rollbacks
                System.out.println("Transaction " + txn.getID() + " aborted. Cascading Rollcacks and retries");
                for (Map.Entry<Integer, Integer> entry : oldValues.entrySet()) {
                    rows[entry.getKey()].setValue(entry.getValue());
                }
            } else {
                System.out.println("Transaction " + txn.getID() + " succeed.");
                success = true; // Transaction completed successfully
            }

            // Release locks
            for (Map.Entry<Integer, Boolean> entry : rid2Locked.entrySet()) {
                if (rid2LockType.get(entry.getKey()) == 0) {
                    locks.get(entry.getKey()).readLock().unlock();
                } else {
                    locks.get(entry.getKey()).writeLock().unlock();
                }
            }
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