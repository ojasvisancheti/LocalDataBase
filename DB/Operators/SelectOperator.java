package ed.inf.adbs.lightdb.Operators;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.Helper.CheckExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Expression;

/**
 * SelectOperator extends Operator abstract class. Every instance of
 * SelectOperator has ScanOperator as its "child" and Expression as "expression"
 * to apply for selection. During evaluation, the SelectOperator’s
 * getNextTuple() method will grab the next tuple from its child (i.e., from the
 * scan), check if that tuple passes the selection expression, and if so output
 * it. If the tuple does not pass the selection expression, the selection
 * operator will continue pulling tuples from the scan until either it finds one
 * that passes or it receives null.
 */
public class SelectOperator extends Operator {

	ScanOperator child = null;
	Expression expression = null;

	/**
	 * @param scan:            Scan Operator
	 * @param expressionTouse: Expression to apply for selection
	 */
	public SelectOperator(ScanOperator scan, Expression expressionTouse) {
		child = scan;
		expression = expressionTouse;
		SchemaAttributes = scan.SchemaAttributes;
	}

	/**
	 * This method will grab the next tuple from its child (i.e., from the scan),
	 * check if that tuple passes the selection expression, and if so output it. If
	 * the tuple does not pass the selection expression, the selection operator will
	 * continue pulling tuples from the scan until either it finds one that passes
	 * or it receives null. We can call getNextTuple() repeatedly to get next
	 * condition satisfying tuple, otherwise it will return null if there will be no
	 * tuple to pass
	 * 
	 * @return Single tuple object or null if no object is available
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = child.getNextTuple();
		while (tuple != null) {
			CheckExpressionVisitorAdapter adapter = new CheckExpressionVisitorAdapter(tuple);
			expression.accept(adapter);
			if (adapter.ExpressionOutput == true) {
				return tuple;
			}

			tuple = child.getNextTuple();
		}

		return null;

	}

	/**
	 * This method tells the operator to reset its child state and start returning
	 * its output again from the beginning; that is, after calling reset() on an
	 * operator, a subsequent call to getNextTuple() will return the expression
	 * satisfying tuple. This functionality is useful if you need to process an
	 * operator’s output multiple times.
	 */
	@Override
	public void reset() {
		child.reset();

	}

}
