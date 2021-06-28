package ed.inf.adbs.lightdb.Helper;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * This class is used to compare the tuple attributes passed, using the
 * condition passed as an expression. We have override the visit method for
 * AndExpression, Column, LongValue, EqualsTo, NotEqualsTo, GreaterThan,
 * GreaterThanEquals, MinorThan and MinorThanEquals. Tuple passed also knows
 * about the schema attribute (R.A, R,B etc) of the tuple values. When
 * expression is passed its visitor override method get called. We have local
 * variable ExpressionOutput which stores overall condition output. "And"
 * Expression Visitor has two expression(left and right) hence it calls other
 * expression visitor like GreaterThanEquals, MinorThan and MinorThanEquals etc
 * which again call Long value or column expression visitor. Long value or
 * column expression visitor store there respective integer value in local
 * variable, goes back to there called operator which perform suitable
 * comparison on there stored value the comparison boolean value get stored in
 * variable ExpressionOutput. Then Again they visit there called And expression
 * and And expression perform And operation on there called left and right
 * expression stored values to get ultimate ExpressionOutput. If there will be
 * more And operation this whole process will get called iteratively. while
 * checking the multiple And operation, if any of the expression output is false
 * the iterative expression evaluation will get stop and false will get stored
 * in "ExpressionOutput".
 */
public class CheckExpressionVisitorAdapter extends ExpressionDeParser {

	public boolean ExpressionOutput = true;
	public Integer Value = 0;

	public Tuple tupleToEvaluate = null;

	/**
	 * @param tuple to compare
	 */
	public CheckExpressionVisitorAdapter(Tuple tuple) {
		super(null, new StringBuilder());
		tupleToEvaluate = tuple;
	}

	/**
	 * visit method for And expression, this will call visitBinaryExpr
	 * 
	 * @param expr: AndExpression
	 */
	@Override
	public void visit(AndExpression expr) {

		visitBinaryExpr(expr);

	}

	/**
	 * visit method for EqualsTo expression. This method call either LongValue or
	 * Column expression, store there integer value as LeftValue and RightValue and
	 * perform comparison on integer value stored
	 * 
	 * @param expr: EqualsTo Expression
	 */
	@Override
	public void visit(EqualsTo expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue == RightValue;

	}

	/**
	 * visit method for NotEqualsTo expression. This method call either LongValue or
	 * Column expression store there integer value as LeftValue and RightValue and
	 * perform comparison on stored integer value
	 * 
	 * @param expr: NotEqualsTo Expression
	 */
	@Override
	public void visit(NotEqualsTo expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue != RightValue;
	}

	/**
	 * visit method for GreaterThan expression. This method call either LongValue or
	 * Column expression store there integer value as LeftValue and RightValue and
	 * perform comparison on stored integer value
	 * 
	 * @param expr: GreaterThan Expression
	 */
	@Override
	public void visit(GreaterThan expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue > RightValue;
	}

	/**
	 * visit method for GreaterThanEquals expression. This method call either
	 * LongValue or Column expression store there integer value as LeftValue and
	 * RightValue and perform comparison on stored integer value
	 * 
	 * @param expr: GreaterThanEquals Expression
	 */
	@Override
	public void visit(GreaterThanEquals expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue >= RightValue;
	}

	/**
	 * visit method for MinorThan expression. This method call either LongValue or
	 * Column expression store there integer value as LeftValue and RightValue and
	 * perform comparison on stored integer value
	 * 
	 * @param expr: MinorThan Expression
	 */
	@Override
	public void visit(MinorThan expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue < RightValue;
	}

	/**
	 * visit method for MinorThanEquals expression. This method call either
	 * LongValue or Column expression store there integer value as LeftValue and
	 * RightValue and perform comparison on stored integer value
	 * 
	 * @param expr: MinorThanEquals Expression
	 */
	@Override
	public void visit(MinorThanEquals expr) {
		expr.getLeftExpression().accept(this);
		Integer LeftValue = Value;
		expr.getRightExpression().accept(this);
		Integer RightValue = Value;
		ExpressionOutput = LeftValue <= RightValue;
	}

	/**
	 * visit method for LongValue expression. This method converts the value passed
	 * into an integer and store it in a temporary local variable "Value"
	 * 
	 * @param expr: LongValue Expression
	 */
	@Override
	public void visit(LongValue expr) {
		Value = Integer.parseInt(expr.toString());

	}

	/**
	 * visit method for Column expression. This method converts the column passed
	 * into an integer using the tuple passed who knows about there schema column
	 * attributes. The resultant integer value is stored in a temporary local
	 * variable "Value"
	 * 
	 * @param expr: Column Expression
	 */
	@Override
	public void visit(Column expr) {
		Value = tupleToEvaluate.TupleElements.get(expr.getFullyQualifiedName().toString());
	}

	/**
	 * visit method for BinaryExpression expression. This method is get called by
	 * And Expression. And Expression will have multiple And expression/ single
	 * expression(>, <. = etc) on left side and single expression on right side. We
	 * first call the visit method for right expression which perform the comparison
	 * and evaluate result. if result evaluate is true then we will evaluate other
	 * And expression iteratively otherwise stop iteration and save false as
	 * expression output.
	 * 
	 * @param expr: Binary Expression
	 */
	private void visitBinaryExpr(BinaryExpression expr) {
		boolean internaloutput = false;
		if (!(expr.getRightExpression() instanceof AndExpression)) {
			internaloutput = ExpressionOutput;
			expr.getRightExpression().accept(this);
			ExpressionOutput = ExpressionOutput && internaloutput;
		} else {
			expr.getRightExpression().accept(this);
		}

		if (ExpressionOutput != false) {
			if (!(expr.getLeftExpression() instanceof AndExpression)) {
				internaloutput = ExpressionOutput;
				expr.getLeftExpression().accept(this);
				ExpressionOutput = ExpressionOutput && internaloutput;

			} else {
				expr.getLeftExpression().accept(this);
			}
		}
	}

}
