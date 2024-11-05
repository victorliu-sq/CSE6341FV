package src;

import java.util.List;
import java.util.LinkedList;

public class Transaction {
    private LinkedList<Operation> operations;
    private int id;

    public Transaction() {
        operations = new LinkedList<Operation>();
    }

    public Transaction(int id) {
        this.id = id;
        operations = new LinkedList<>();
    }

    public int getID() {
        return this.id;
    }

    public void addOperation(Operation o) {
        operations.add(o);
    }

    public List<Operation> getOperations() {
        return this.operations;
    }
} 
