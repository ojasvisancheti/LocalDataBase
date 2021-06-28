package ed.inf.adbs.lightdb.Operators;

import java.util.ArrayList;

import ed.inf.adbs.lightdb.Tuple;

/**
 * Operator is the base class for all the operators created, getNextTuple() and
 * reset() are implemented by every operator. We have added a method dump() to
 * dump the resultant output into a StringBuilder "Output" variable which can be
 * used further to show the results in the form of CSV file. We have also
 * created SchemaAttributes list so that every operator should know on which
 * schema attributes(eg. R.A, R.B etc) its working on, This makes the overall
 * computation easy and manageable
 */
abstract public class Operator {

	public StringBuilder Output = new StringBuilder();
	public ArrayList<String> SchemaAttributes = new ArrayList<String>();

	/**
	 * Every derived operator must implement this method.The idea is that once you
	 * create a relational operator, you can call getNextTuple() repeatedly to get
	 * the next tuple of the operator’s output. This is sometimes called “pulling
	 * tuples” from the operator. If the operator still has some available output,
	 * it will return the next tuple, otherwise it should return null.
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Every derived operator must implement this method.This Method tells the
	 * operator to reset its state and start returning its output again from the
	 * beginning; that is, after calling reset() on an operator, a subsequent call
	 * to getNextTuple() will return the first tuple in that operator’s output, even
	 * though the tuple may have been returned before. This functionality is useful
	 * if you need to process an operator’s output multiple times, e.g., for
	 * scanning the inner relation multiple times during a join.
	 */
	public abstract void reset();

	/**
	 * This method is used by resultant operator if we need to know the resultant
	 * output table.This method repeatedly calls getNextTuple() until the next tuple
	 * is null (no more output) and writes each tuple to a StringBuilder "Output"
	 * That way you can dump() the results of any operator – including the root of
	 * your query plan.
	 */
	public void dump() {
		reset();

		Output = new StringBuilder();

		Tuple element = getNextTuple();
		if (element != null) {
			String[] keys = element.TupleElements.keySet().toArray(new String[0]);
			while (element != null) {
				for (int i = 0; i < keys.length; i++) {
					Output.append(element.TupleElements.get(keys[i]));
					Output.append(",");
				}

				Output.deleteCharAt(Output.lastIndexOf(","));
				Output.append("\n");
				element = getNextTuple();
			}
		}

		System.out.println(Output.toString());

	}

}
