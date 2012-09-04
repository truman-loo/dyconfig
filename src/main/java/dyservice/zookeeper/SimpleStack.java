package dyservice.zookeeper;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;

public class SimpleStack<T> {
	private List<T> container = new ArrayList<T>();

	// Tests if this stack is empty.
	public final boolean empty() {
		return container.isEmpty();
	}

	// Looks at the object at the top of this stack without removing it
	// from the stack.
	public final T peek() {
		if (empty())
			throw new EmptyStackException();
		return container.get(container.size() - 1);
	}

	// Removes the object at the top of this stack and returns that
	// object as the value of this function.
	public final T pop() {
		return container.remove(container.size() - 1);
	}


	// Pushes an item onto the top of the stack
	public final T push(T item) {
		container.add(item);
		return item;
	}
}
