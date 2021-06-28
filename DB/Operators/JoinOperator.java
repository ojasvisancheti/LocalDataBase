package ed.inf.adbs.lightdb.Operators;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.Helper.CheckExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Expression;

/**
 * JoinOperator extends Operator abstract class. Every instance of JoinOperator
 * has both a left and right child Operator. It also has an Expression
 * "expression" which captures the join condition. During evaluation, it scan
 * the left (outer) child once, and for each tuple in the outer it scans the
 * inner child(right) completely (do reset for next iteration of left child).
 * Once the operator has obtained a tuple from the outer and a tuple from the
 * inner, it glues them together. If there is a non-null join condition it
 * returns the tuple directly, otherwise the tuple is only returned if it
 * matches the join condition.
 */
public class JoinOperator extends Operator {

	Operator leftChild = null;
	Operator rightChild = null;
	Expression expression = null;
	Tuple tuple2 = null;
	Tuple tuple1 = null;

	/**
	 * @param operator1       :Left child
	 * @param operator2       :Right Child
	 * @param expressionTouse :Expression to join can be null
	 */
	public JoinOperator(Operator operator1, Operator operator2, Expression expressionTouse) {
		leftChild = operator1;
		rightChild = operator2;
		expression = expressionTouse;
		// keep tracks of join schema attributes
		SchemaAttributes.addAll(leftChild.SchemaAttributes);
		SchemaAttributes.addAll(rightChild.SchemaAttributes);

	}

	/**
	 * This method implement the simple nested loop join algorithm, The join scans
	 * the left child once, and for each tuple in the Left child, it scans the right
	 * child completely (finally a use for the reset() method!). Once the operator
	 * has obtained a tuple from the Left child and a tuple from the right child, it
	 * glues them together. If there is a non-null join condition, the tuple is only
	 * returned if it matches the join condition. If the join is a cross product i.e
	 * "expression" is null, all pairs of tuples are returned.
	 * 
	 * @return Single tuple object or null if no object is available
	 */
	@Override
	public Tuple getNextTuple() {
		// GetNextTuple should be perform only one time with leftChild after that it
		// only get performed when rightChild has no element to iterate
		if (tuple1 == null) {
			tuple1 = leftChild.getNextTuple();
			rightChild.reset();
		}
		while (tuple1 != null) {
			tuple2 = rightChild.getNextTuple();

			while (tuple2 != null) {
				Tuple newTuple = new Tuple();
				newTuple.TupleElements.putAll(tuple1.TupleElements);
				newTuple.TupleElements.putAll(tuple2.TupleElements);
				if (expression != null) {
					CheckExpressionVisitorAdapter adapter = new CheckExpressionVisitorAdapter(newTuple);
					expression.accept(adapter);
					if (adapter.ExpressionOutput == true) {
						return newTuple;
					}
				} else {
					return newTuple;
				}

				tuple2 = rightChild.getNextTuple();
			}

			tuple1 = leftChild.getNextTuple();
			rightChild.reset();
		}

		return null;
	}

	/**
	 * This method tells the operator to reset the left child and right child state
	 * and start returning its output again from the beginning; that is, after
	 * calling reset() on an operator, a subsequent call to getNextTuple()
	 */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();

	}

}
