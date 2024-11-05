package src;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public final class Permutation {
    public static List<List<Integer>> getAllPermutations(List<Integer> numbers) {
        List<List<Integer>> result = new ArrayList<>();
        generatePermutations(numbers, 0, result);
        return result;
    }

    private static void generatePermutations(List<Integer> numbers, int index, List<List<Integer>> result) {
        if (index == numbers.size() - 1) {
            // Add a copy of the current permutation to the result list
            result.add(new ArrayList<>(numbers));
            return;
        }

        for (int i = index; i < numbers.size(); i++) {
            // Swap numbers[i] and numbers[index]
            Collections.swap(numbers, i, index);

            // Recursively generate the remaining permutations
            generatePermutations(numbers, index + 1, result);

            // Swap back to restore the original order
            Collections.swap(numbers, i, index);
        }
    }

    public static void main(String[] args) {
        // Create a list with numbers from 1 to 10
        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            numbers.add(i);
        }

        // Generate all permutations
        List<List<Integer>> allPermutations = getAllPermutations(numbers);

        // Print all permutations
        for (List<Integer> permutation : allPermutations) {
            System.out.println(permutation);
        }
    }
}
