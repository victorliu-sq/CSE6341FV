package src;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class Serializability {
    public static boolean checkSerializable(int numRows, List<Transaction> txns, List<Transaction> actualTxns, Row[] actualRows) {
        // Create a list with numbers from 1 to 10
        List<Integer> numbers = new ArrayList<>();
        for (int i = 0; i < txns.size(); i++) {
            numbers.add(i);
        }

        // Generate all permutations
        List<List<Integer>> allPermutations = Permutation.getAllPermutations(numbers);

        // Print all permutations
        boolean isSerializable = false;
        for (List<Integer> permutation : allPermutations) {
            List<Transaction> txnsCopy = new LinkedList<>(txns);

            Row[] serialRows = new Row[numRows];
            System.out.println(permutation);
            for (int i = 0; i < numRows; i++) {
                serialRows[i] = new Row(i);
            }

            // exeucute TXNs
            for (int txnID : permutation) {
                Transaction txn = txnsCopy.get(txnID);
                for (Operation o : txn.getOperations()) {
                    if (o.getType() == 0) {
                        o.setValue(serialRows[o.getRowNumber()].getValue());
                    } else {
                        serialRows[o.getRowNumber()].setValue(o.getValue());
                    }
                }
            }

            // Check serilizability
            boolean isReadSerializable = true;
            int i = 0;
            while (i < txns.size() && isReadSerializable) {
                List<Operation> serialOperations = txnsCopy.get(i).getOperations();
                List<Operation> actualOperations = actualTxns.get(i).getOperations();
                int j = 0;
                while (j < actualOperations.size() && isReadSerializable) {
                    Operation serialOperation = serialOperations.get(j);
                    Operation actualOperation = actualOperations.get(j);
                    if (serialOperation.getType() == 0 && serialOperation.getValue() != actualOperation.getValue()) {
                        isReadSerializable = false;
                    }
                    j++;
                }
                i++;
            }

            boolean isWriteSerializable = true;
            i = 0;
            while (i < serialRows.length && isWriteSerializable) {
                if (serialRows[i].getValue() != actualRows[i].getValue()) {
                    isWriteSerializable = false;
                }
                i++;
            }

            isSerializable = isReadSerializable && isWriteSerializable;
            if (isSerializable) {
                System.out.print("This This execution is equivalent to a serial execution of ");
                for (i = 0; i < txns.size() - 1; i++) {
                    System.out.print("TXN " + txns.get(i).getID() + " -> ");
                }
                System.out.println("TXN " + txns.get(txns.size() - 1).getID());
                break;
            }
        }

        return isSerializable;
    }
}
