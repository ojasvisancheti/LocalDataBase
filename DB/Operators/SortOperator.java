package ed.inf.adbs.lightdb.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.Helper.TupleComparator;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * SortOperator extends Operator abstract class. Every instance of SortOperator
 * knows about Operator to sort "child" and by which element it should be
 * sorted. SortOperator is going to read all of the output from its child, place
 * it into an internal buffer "TuplesBuffer", sort it, and then return
 * individual tuples when requested. We have implemented by default only
 * ascending order of sorting
 */
public class SortOperator extends Operator {

	public List<Tuple> TuplesBuffer = new ArrayList<Tuple>();
	Integer pointer = 0;

	/**
	 * "TuplesBuffer" contains all tuples . TupleComparator a custom comparator is
	 * used to sort the tuples in "TuplesBuffer"
	 * 
	 * @param child          :Operator passed to sort
	 * @param elementToOrder :Element used for sorting Buffer object
	 */
	public SortOperator(Operator child, List<OrderByElement> orderbyelements) {
		child.reset();
		Tuple tuple = child.getNextTuple();
		while (tuple != null) {
			TuplesBuffer.add(tuple);
			tuple = child.getNextTuple();
		}
		if (orderbyelements != null) {
			Collections.sort(TuplesBuffer, new TupleComparator(orderbyelements));
		}
	}

	/**
	 * This method will it grabs the next tuple from the buffer and returns it. We
	 * can call getNextTuple() repeatedly to get next tuple, otherwise it will
	 * return null if buffer has no more tuple to return.The pointer keep tracks of
	 * which tuple is passed.
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
