package ed.inf.adbs.lightdb.Operators;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import ed.inf.adbs.lightdb.Tuple;

/**
 * DuplicateEliminationOperator extends Operator abstract class. Every instance
 * of DuplicateEliminationOperator knows about SortOperator "child". it reads
 * the tuples from the child into TuplesBuffer , remove duplicates and return
 * individual tuples when requested.
 * 
 */
public class DuplicateEliminationOperator extends Operator {

	public ArrayList<Tuple> TuplesBuffer = new ArrayList<Tuple>();
	Integer pointer = 0;
	SortOperator child = null;

	/**
	 * * "TuplesBuffer" contains all tuples from child . LinkedHashSet is used to
	 * eliminate all the duplicate from the "TuplesBuffer"
	 * 
	 * @param operator: SortOperator as child
	 */
	public DuplicateEliminationOperator(SortOperator operator) {
		child = operator;
		child.reset();
		Tuple tuple = child.getNextTuple();
		while (tuple != null) {
			TuplesBuffer.add(tuple);
			tuple = child.getNextTuple();
		}

		LinkedHashSet<Tuple> hashSet = new LinkedHashSet<>(TuplesBuffer);
		TuplesBuffer = new ArrayList<Tuple>(hashSet);
	}

	/**
	 * This method will grabs the next tuple from the buffer and returns it. We can
	 * call getNextTuple() repeatedly to get next tuple, otherwise it will return
	 * null if buffer has no more tuple to return.The pointer keep tracks of which
	 * tuple is passed.
	 * 
	 * @return Single tuple object or null if no object is available
	 */
	@Override
	public Tuple getNextTuple() {

		if (pointer < TuplesBuffer.size()) {

			Tuple tupleToReturn = TuplesBuffer.get(pointer);
			pointer = pointer + 1;
			return tupleToReturn;
		} else {
			return null;
		}

	}

	/**
	 * This method tells the operator to reset pointer to the first tuple, that is,
	 * after calling reset() on an operator, a subsequent call to getNextTuple()
	 * will return the first buffer tuple. This functionality is useful if you need
	 * to process an operator’s output multiple times.
	 */
	@Override
	public void reset() {
		pointer = 0;

	}

}
