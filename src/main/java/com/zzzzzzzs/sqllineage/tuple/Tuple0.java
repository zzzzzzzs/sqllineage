package com.zzzzzzzs.sqllineage.tuple;

import java.io.ObjectStreamException;

public class Tuple0 extends Tuple {
	private static final long serialVersionUID = 1L;

	// an immutable reusable Tuple0 instance
	public static final Tuple0 INSTANCE = new Tuple0();

	// ------------------------------------------------------------------------

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public <T> T getField(int pos) {
		throw new IndexOutOfBoundsException(String.valueOf(pos));
	}

	@Override
	public <T> void setField(T value, int pos) {
		throw new IndexOutOfBoundsException(String.valueOf(pos));
	}

	/**
	 * Shallow tuple copy.
	 * @return A new Tuple with the same fields as this.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Tuple0 copy(){
		return new Tuple0();
	}

	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form "()".
	 *
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "()";
	}

	/**
	 * Deep equality for tuples by calling equals() on the tuple members.
	 *
	 * @param o
	 *            the object checked for equality
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof Tuple0;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	// singleton deserialization
	private Object readResolve() throws ObjectStreamException {
		return INSTANCE;
	}
}
