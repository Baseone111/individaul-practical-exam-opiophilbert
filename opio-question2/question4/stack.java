import java.util.*;

class Stack<T> {
    private List<T> elements = new ArrayList<>();

    // Push an item onto the stack
    public void push(T item) {
        elements.add(item);
    }

    // Pop an item from the stack
    public T pop() {
        if (elements.isEmpty()) {
            throw new EmptyStackException();
        }
        return elements.remove(elements.size() - 1);
    }

    // View top element without removing
    public T top() {
        if (elements.isEmpty()) {
            throw new EmptyStackException();
        }
        return elements.get(elements.size() - 1);
    }

    // Check if empty
    public boolean isEmpty() {
        return elements.isEmpty();
    }
}