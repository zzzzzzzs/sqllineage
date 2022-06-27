package com.zzzzzzzs.sqllineage.tuple;

public class Tuple3<T0, T1, T2> extends Tuple {

  private static final long serialVersionUID = 1L;

  /** Field 0 of the tuple. */
  public T0 f0;
  /** Field 1 of the tuple. */
  public T1 f1;
  /** Field 2 of the tuple. */
  public T2 f2;

  /** Creates a new tuple where all fields are null. */
  public Tuple3() {}

  /**
   * Creates a new tuple and assigns the given values to the tuple's fields.
   *
   * @param value0 The value for field 0
   * @param value1 The value for field 1
   * @param value2 The value for field 2
   */
  public Tuple3(T0 value0, T1 value1, T2 value2) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
  }

  @Override
  public int getArity() {
    return 3;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getField(int pos) {
    switch (pos) {
      case 0:
        return (T) this.f0;
      case 1:
        return (T) this.f1;
      case 2:
        return (T) this.f2;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void setField(T value, int pos) {
    switch (pos) {
      case 0:
        this.f0 = (T0) value;
        break;
      case 1:
        this.f1 = (T1) value;
        break;
      case 2:
        this.f2 = (T2) value;
        break;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  /**
   * Sets new values to all fields of the tuple.
   *
   * @param value0 The value for field 0
   * @param value1 The value for field 1
   * @param value2 The value for field 2
   */
  public void setFields(T0 value0, T1 value1, T2 value2) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
  }

  // -------------------------------------------------------------------------------------------------
  // standard utilities
  // -------------------------------------------------------------------------------------------------

  /**
   * Creates a string representation of the tuple in the form (f0, f1, f2), where the individual
   * fields are the value returned by calling {@link Object#toString} on that field.
   *
   * @return The string representation of the tuple.
   */
  @Override
  public String toString() {
    return "("
        + TupleUtils.arrayAwareToString(this.f0)
        + ","
        + TupleUtils.arrayAwareToString(this.f1)
        + ","
        + TupleUtils.arrayAwareToString(this.f2)
        + ")";
  }

  /**
   * Deep equality for tuples by calling equals() on the tuple members.
   *
   * @param o the object checked for equality
   * @return true if this is equal to o.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tuple3)) {
      return false;
    }
    @SuppressWarnings("rawtypes")
    Tuple3 tuple = (Tuple3) o;
    if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
      return false;
    }
    if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) {
      return false;
    }
    if (f2 != null ? !f2.equals(tuple.f2) : tuple.f2 != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = f0 != null ? f0.hashCode() : 0;
    result = 31 * result + (f1 != null ? f1.hashCode() : 0);
    result = 31 * result + (f2 != null ? f2.hashCode() : 0);
    return result;
  }

  /**
   * Shallow tuple copy.
   *
   * @return A new Tuple with the same fields as this.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Tuple3<T0, T1, T2> copy() {
    return new Tuple3<>(this.f0, this.f1, this.f2);
  }

  /**
   * Creates a new tuple and assigns the given values to the tuple's fields. This is more convenient
   * than using the constructor, because the compiler can infer the generic type arguments
   * implicitly. For example: {@code Tuple3.of(n, x, s)} instead of {@code new Tuple3<Integer,
   * Double, String>(n, x, s)}
   */
  public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 value0, T1 value1, T2 value2) {
    return new Tuple3<>(value0, value1, value2);
  }
}
