
// Question 3b: Generic Stack in Java

public class Main {
    public static void main(String[] args) {
        // Stack of Integers
        Stack<Integer> intStack = new Stack<>();
        intStack.push(10);
        intStack.push(20);
        System.out.println("Top Integer: " + intStack.top());
        System.out.println("Popped: " + intStack.pop());

        // Stack of Strings
        Stack<String> strStack = new Stack<>();
        strStack.push("Hello");
        strStack.push("World");
        System.out.println("Top String: " + strStack.top());
        System.out.println("Popped: " + strStack.pop());
    }
}