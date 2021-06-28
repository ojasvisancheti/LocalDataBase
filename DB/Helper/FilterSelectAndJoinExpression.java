package ed.inf.adbs.lightdb.Helper;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
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
 * This class is used to filter expression according to the schema attribute
 * passed(R.A, R,B etc). The filtered expression will get stored in
 * "ExpressionTouse". We have also passed alreadyuseExpressionList in this class
 * so the "ExpressionTouse" will not contains the already used
 * expression(example select expression as it is already used to create Select
 * operator). We have override the visit method for AndExpression, Column,
 * LongValue, EqualsTo, NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan
 * and MinorThanEquals. "And" Expression has two expression(left and right)
 * hence it calls other expression visitor like reaterThanEquals, MinorThan and
 * MinorThanEquals etc which again call Long value or column expression visitor.
 * Long value or column expression visitor store boolean "SelectExpression" as
 * true if the passed expression(column) belongs to the schema attribute list
 * and goes back to there called operator which again check if there left and
 * right expression values which are either LongValue or Column Name both
 * belongs to the attribute passed(evaluated true), if yes they will ask
 * "AddExpression" to add it in Expression to use with or without 'AND'
 * Expression. If there will be more And operation this whole process will get
 * called iteratively.
 */
public class FilterSelectAndJoinExpression extends ExpressionDeParser {

	public boolean SelectExpression = true;

	public Expression ExpressionTouse = null;
	List<String> attributesTouse = null;
	public List<Expression> UseExpressionList = new ArrayList<>();
	List<Expression> AlreadyuseExpressionList = null;

	/**
	 * @param attributes                : Schema attributes to filter expression
	 * @param alreadyuseExpressionList: Expression not be to consider as its already
	 *                                  used
	 */
	public FilterSelectAndJoinExpression(List<String> attributes, List<Expression> alreadyuseExpressionList) {
		super(null, new StringBuilder());

		attributesTouse = attributes;
		AlreadyuseExpressionList = alreadyuseExpressionList;

	}

	/**
	 * visit method for And expression This will call visit method for binary
	 * expression
	 */
	@Override
	public void visit(AndExpression expr) {

		visitBinaryExpr(expr);

	}

	/**
	 * visit method for EqualsTo expression. This method call either LongValue or
	 * Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: EqualsTo Expression
	 */
	@Override
	public void visit(EqualsTo expr) {

		expr.getLeftExpression().accept(this);
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}

	}

	/**
	 * visit method for NotEqualsTo expression. This method call either LongValue or
	 * Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: NotEqualsTo Expression
	 */
	@Override
	public void visit(NotEqualsTo expr) {
		expr.getLeftExpression().accept(this);
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}

	}

	/**
	 * visit method for GreaterThan expression. This method call either LongValue or
	 * Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: GreaterThan Expression
	 */
	@Override
	public void visit(GreaterThan expr) {

		expr.getLeftExpression().accept(this);
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}
	}

	/**
	 * visit method for GreaterThanEquals expression. This method call either
	 * LongValue or Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: GreaterThanEquals Expression
	 */
	@Override
	public void visit(GreaterThanEquals expr) {
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}
	}

	/**
	 * visit method for MinorThan expression. This method call either LongValue or
	 * Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: MinorThan Expression
	 */
	@Override
	public void visit(MinorThan expr) {
		expr.getLeftExpression().accept(this);
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}
	}

	/**
	 * visit method for MinorThanEquals expression. This method call either
	 * LongValue or Column expression store there boolean value as leftSelection and
	 * rightSelection and if both are true i.e they both belongs to the schema
	 * attribute passed then it ask "AddExpression" to add it in the expression to
	 * use
	 * 
	 * @param expr: MinorThanEquals Expression
	 */
	@Override
	public void visit(MinorThanEquals expr) {
		expr.getLeftExpression().accept(this);
		boolean leftSelection = SelectExpression;
		expr.getRightExpression().accept(this);
		boolean rightSelection = SelectExpression;
		if (leftSelection && rightSelection) {
			AddExpression(expr);
		}

	}

	/**
	 * visit method for LongValue expression. This method always return true as its
	 * a constant so it doesn't need to check attribute list
	 * 
	 * @param expr: LongValue Expression
	 */
	@Override
	public void visit(LongValue expr) {
		SelectExpression = true;

	}

	/**
	 * visit method for Column expression. This method check if the column passed
	 * belongs to the attribute list if yes then stores true in "SelectExpression"
	 * otherwise store false.
	 * 
	 * @param expr: LongValue Expression
	 */
	@Override
	public void visit(Column expr) {
		if (attributesTouse.contains(expr.toString())) {
			SelectExpression = true;
		} else {
			SelectExpression = false;
		}
	}

	/**
	 * visit method for BinaryExpression expression. This method is get called by
	 * And Expression. And Expression will have multiple And expression/single
	 * expression(>,<, etc) on left side and on right side. We first call the visit
	 * method for right expression which perform the comparison and evaluate result.
	 * Then we call visit method for Left expression which either again call And
	 * expression or single expression visit method. This operation will perform
	 * iteratively for all left and Right expression of AND expression
	 * 
	 * @param expr: Binary Expression
	 */
	private void visitBinaryExpr(BinaryExpression expr) {
		if (!(expr.getRightExpression() instanceof AndExpression)) {
			expr.getRightExpression().accept(this);
		} else {
			expr.getRightExpression().accept(this);
		}

		if (!(expr.getLeftExpression() instanceof AndExpression)) {
			expr.getLeftExpression().accept(this);

		} else {
			expr.getLeftExpression().accept(this);
		}

	}

	/**
	 * AddExpression method is used by all single expression( >, <, = etc) when
	 * there right and left expression(column/Long value) belongs to the passed
	 * schema attribute list. This AddExpression will check first the expression
	 * passed is already present in the"AlreadyuseExpressionList", if yes then it
	 * will discard it. If Not It will further check if "UseExpressionList" contains
	 * any other expression if yes it will create a new "ExpressionTouse" by joining
	 * old and passed expression "expr" by And operator. If "UseExpressionList" is
	 * empty then it will directly allocate the "expr" passed as "ExpressionTouse"
	 * 
	 * @param expr: LongValue Expression
	 */
	private void AddExpression(Expression expr) {

		if (AlreadyuseExpressionList != null) {
			if (AlreadyuseExpressionList.contains(expr)) {
				return;
			}
		}
		UseExpressionList.add(expr);
		if (ExpressionTouse == null) {
			ExpressionTouse = expr;
		} else {
			ExpressionTouse = new AndExpression(ExpressionTouse, expr);
		}

	}

}
