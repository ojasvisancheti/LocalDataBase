package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import ed.inf.adbs.lightdb.Helper.FilterSelectAndJoinExpression;
import ed.inf.adbs.lightdb.Operators.DuplicateEliminationOperator;
import ed.inf.adbs.lightdb.Operators.JoinOperator;
import ed.inf.adbs.lightdb.Operators.Operator;
import ed.inf.adbs.lightdb.Operators.ProjectionOperator;
import ed.inf.adbs.lightdb.Operators.ScanOperator;
import ed.inf.adbs.lightdb.Operators.SelectOperator;
import ed.inf.adbs.lightdb.Operators.SortOperator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * This Build query create a query plan by taking Sql query and parsing it. Plan
 * will be in the form, A non-optional scan operator. An optional selection
 * operator, having a child as Operator. An optional projection operator, having
 * a child as Operator. An optional join operator, having left and right child
 * as Operator. An optional sort operator having a child as Operator. An
 * optional duplicate elimination operator having a child as compulsory sort
 * Operator.
 */
public class BuildQuery {
	public Statement statement;
	String outputFile = "samples" + File.separator + "expected_output" + File.separator + "query9.csv";
	SelectOperator selectclause = null;
	ScanOperator[] scanTables = null;
	List<Column> coloumsToProject = null;
	public Operator ResultOperator = null;
	SortOperator sortOperator = null;

	/**
	 * @param filename : the file containing the sql query to parse and create query
	 *                 plan
	 * @throws IOException
	 * @throws JSQLParserException
	 */
	public BuildQuery(String filename) throws IOException, JSQLParserException {

		// Parse the query
		statement = CCJSqlParserUtil.parse(new FileReader(filename));

		BuildQueryUsingStatement(statement);

	}

	/**
	 * @param statementToExecute: The parsed statement from jsqlparser to create a
	 *                            query plan
	 * @throws IOException
	 */
	public BuildQuery(Statement statementToExecute) throws IOException {

		BuildQueryUsingStatement(statementToExecute);
	}

	/**
	 * This method parse the query statement and simultaneously create a query plan
	 * The query plan will be created as follows 1) A non-optional scan operator is
	 * created using table name and alias obtained from "GetTableToScan()". 2) Non
	 * optional Join operator is created using "GetResultantJoinOperator". 3) if no
	 * join tables are available then simple optional selection operator is created
	 * using expression from "GetWhereExpression()" 4) optional ProjectionOperato
	 * get created using columns to project from GetSelectProjection() 5) optional
	 * SortOperator get created using order by element from
	 * "GetSortOperatorCondition".6)optional DuplicateEliminationOperator is created
	 * when HaveDistint() returns true, it is compulsory to have SortOperator while
	 * creating DuplicateEliminationOperator, as we have implemented SortOperator as
	 * a block operator which helps to remove distinct elements
	 * 
	 * @param statementToExecute : statement use to create a query plan
	 * @throws FileNotFoundException
	 */
	public void BuildQueryUsingStatement(Statement statementToExecute) throws FileNotFoundException {
		statement = statementToExecute;
		if (statement != null) {
			System.out.println("Read statement: " + statement);
			// Scan operator
			AbstractMap.SimpleEntry<String, String> TableWithAlais = GetTableToScan();
			ScanOperator scanTable = new ScanOperator(TableWithAlais.getKey(), TableWithAlais.getValue());

			ResultOperator = scanTable;
			Expression whereExpression = GetWhereExpression();
			// Join clause
			HashMap<String, String> JointableWithAlais = GetJoinTablesToScanAlongWithAlais();
			if (JointableWithAlais != null) {
				ResultOperator = GetResultantJoinOperator(scanTable, JointableWithAlais, whereExpression);
			} else {

				// Where Clause
				SelectOperator selectclause = null;
				if (whereExpression != null) {
					selectclause = new SelectOperator(scanTable, GetWhereExpression());
					ResultOperator = selectclause;
				}
			}

			// Projection
			coloumsToProject = GetSelectProjection();
			ProjectionOperator projection = null;
			if (coloumsToProject != null && coloumsToProject.isEmpty() == false) {

				projection = new ProjectionOperator(ResultOperator, coloumsToProject);
				ResultOperator = projection;
			}

			// Order By
			List<OrderByElement> orderbyelements = GetSortOperatorCondition();
			if (orderbyelements != null && orderbyelements.isEmpty() == false) {
				sortOperator = new SortOperator(ResultOperator, orderbyelements);
				ResultOperator = sortOperator;
			}

			// Distinct Operator
			boolean isDistint = HaveDistint();
			if (isDistint == true) {
				if (sortOperator == null) {
					sortOperator = new SortOperator(ResultOperator, null);
				}
				DuplicateEliminationOperator duplicateElimination = new DuplicateEliminationOperator(sortOperator);
				ResultOperator = duplicateElimination;
			}
		}
	}

	/**
	 * This method parser and return select expression obtained from the getWhere()
	 * method of PlainSelect of JSQLParser.
	 * 
	 * @return returns a select expression using JSQLParser PlainSelect
	 */
	public Expression GetWhereExpression() {

		Select select = (Select) statement;

		if (select != null) {
			PlainSelect plain = (PlainSelect) select.getSelectBody();
			if (plain.getWhere() != null) {
				Expression expr = (Expression) plain.getWhere();
				return expr;
			}
		}

		return null;
	}

	/**
	 * This method parser and return projection columns obtained from the
	 * selectItems field of PlainSelect of JSQLParser. "selectitems" is a list of
	 * SelectItems, where each one is a column to project
	 * 
	 * @return list of columns to project
	 */
	public List<Column> GetSelectProjection() {
		List<Column> projectionColoums = new ArrayList<Column>();
		Select select = (Select) statement;
		if (select != null) {

			PlainSelect plain = (PlainSelect) select.getSelectBody();
			List selectitems = plain.getSelectItems();
			List<String> list = new ArrayList<String>();
			list.stream().forEach(c -> System.out.println(c));
			for (int i = 0; i < selectitems.size(); i++) {
				if (selectitems.get(i) instanceof SelectExpressionItem) {
					Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
					if (expression instanceof Column) {
						Column col = (Column) expression;
						projectionColoums.add(col);

					} else {
						return null;
					}
				}

			}
		}

		return projectionColoums;
	}

	/**
	 * This method use JSQLParser to read the PlainSelect then by using getJoins()
	 * we will get the list of all join tables(Table Name and alias). We have used
	 * tablesNamesFinder to get all table names and this table names are used to
	 * filter the alias and the actual join table name when we get the combination
	 * of them using "getJoins()"
	 * 
	 * @return HashMap containing join Table name as key and alias(null if not
	 *         present) as value
	 */
	public HashMap<String, String> GetJoinTablesToScanAlongWithAlais() {
		HashMap<String, String> tableWithAlais = null;
		Select select = (Select) statement;
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableNames = tablesNamesFinder.getTableList(select);

		PlainSelect ps = (PlainSelect) select.getSelectBody();
		String alaisName = null;

		List joinlist = ps.getJoins();
		if (joinlist != null) {
			tableWithAlais = new LinkedHashMap<String, String>();
			for (int i = 0; i < joinlist.size(); i++) {
				Join j = ps.getJoins().get(i);
				alaisName = null;
				if (ps.getJoins().get(i).getRightItem().getAlias() != null) {
					alaisName = ps.getJoins().get(i).getRightItem().getAlias().getName();
				}
				for (Integer k = 0; k < tableNames.size(); k++) {
					if (ps.getJoins().get(i).toString().contains(tableNames.get(k))) {
						if (alaisName == null) {
							tableWithAlais.put(tableNames.get(k), null);
						} else {
							tableWithAlais.put(tableNames.get(k), alaisName);
						}

					}
				}
			}
		}
		return tableWithAlais;

	}

	/**
	 * All table Names are collected using JSQLParser TablesNamesFinder(), to get
	 * the primary table, select getFromItem is used. alias and table name are
	 * separated using table names collected.
	 * 
	 * @return AbstractMap with key as table Name to scan and value as alias(null if
	 *         not present)
	 */
	public AbstractMap.SimpleEntry<String, String> GetTableToScan() {

		Select select = (Select) statement;
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableNames = tablesNamesFinder.getTableList(select);

		PlainSelect ps = (PlainSelect) select.getSelectBody();
		String alaisName = null;
		if (ps.getFromItem().getAlias() != null) {
			alaisName = ps.getFromItem().getAlias().getName();
		}
		for (Integer i = 0; i < tableNames.size(); i++) {
			if (ps.getFromItem().toString().contains(tableNames.get(i))) {
				if (alaisName == null) {

					return new AbstractMap.SimpleEntry<>(tableNames.get(i), null);
				} else {

					return new AbstractMap.SimpleEntry<>(tableNames.get(i), alaisName);
				}

			}
		}

		return null;
	}

	/**
	 * We are creating here a left deep join tree. We have used 3 major conditions
	 * to perform join operation 1) each operator should know which schema
	 * attributes they are working on, this makes the join operation easy and
	 * maintainable. 2) We will make a list of used expression , this list will help
	 * us to not use same expression again and again it will help to reduce
	 * redundant condition check.3) We will use FilterSelectExpression which extends
	 * ExpressionDeParser used to get condition(selection/join) with respect to
	 * schema attribute passed. list of used expression will also get passed to
	 * FilterSelectExpression so it will not return the expression present in the
	 * used list
	 * 
	 * We will first take scan operator ask FilterSelectExpression is there is any
	 * selection condition with respect to operator working schema attributes if yes
	 * we will create Selection operator using that condition and used condition
	 * will added in the list of used expression. This Selection operator/ Scan
	 * operator will act as a left child.
	 * 
	 * We will iterate over join tables collected using parser. for each join table
	 * we create a scan operator then ask FilterSelectExpression is there is any
	 * selection condition with respect to operator working schema attributes if yes
	 * we will create Selection operator using that condition and used condition
	 * will added in the list of used expression. This created Selection operator/
	 * Scan act as a right child
	 * 
	 * we again ask FilterSelectExpression is there is any join condition with
	 * respect to the attributes of left and right child and create Join operator
	 * using that condition. Used join condition will added in the list of used
	 * expression. Now this join Operator created is used as a Left child and next
	 * iterated join table will check selection condition again this scan operator/
	 * Selection Operator will be a right child.
	 * 
	 * This process will continue till all join tables will get iterated.
	 * 
	 * @param scanTable          : This is the first table from "getFromItem()"
	 * @param jointableWithAlais : This has the list of all join tables other than
	 *                           from item one
	 * @param whereclause        : This has whole where clause passed
	 * @return This will return a head join operator
	 * @throws FileNotFoundException
	 */
	public Operator GetResultantJoinOperator(ScanOperator scanTable, HashMap<String, String> jointableWithAlais,
			Expression whereclause) throws FileNotFoundException {

		Operator LeftOperator = null;
		Operator RightOperator = null;
		List<Expression> usedExpressionList = new ArrayList<Expression>();

		/////////////// Creation for Select Operator for Base Table /////////////

		// This will filter select expression/Where clause according to the attribute
		// passed
		FilterSelectAndJoinExpression selectionexpression = new FilterSelectAndJoinExpression(
				scanTable.SchemaAttributes, usedExpressionList);

		if (whereclause != null) {
			whereclause.accept(selectionexpression);
		}

		// We are checking if any expression is present for select expression if yes
		// create a select operator
		if (selectionexpression.ExpressionTouse != null) {
			selectclause = new SelectOperator(scanTable, selectionexpression.ExpressionTouse);
			LeftOperator = selectclause;
			// add expression which is already used in the usedExpression list so we dont
			// need to apply it again
			usedExpressionList.addAll(selectionexpression.UseExpressionList);
		} else {
			LeftOperator = scanTable;
		}

		List<String> keySet = new ArrayList<>(jointableWithAlais.keySet());
		int sizevalue = keySet.size();
		ScanOperator[] scanTables = new ScanOperator[sizevalue];
		// iterate over all join tables
		for (int i = 0; i < keySet.size(); i++) {

			/////////////// Creation for Select Operator //////////////////////////

			// create scan operator for each join table
			scanTables[i] = new ScanOperator(keySet.get(i), jointableWithAlais.get(keySet.get(i)));
			// extract expression associated with select condition for only this join table
			selectionexpression = new FilterSelectAndJoinExpression(scanTables[i].SchemaAttributes, usedExpressionList);
			if (whereclause != null) {
				whereclause.accept(selectionexpression);
			}

			// if Expression is present create select operator
			if (selectionexpression.ExpressionTouse != null) {
				selectclause = new SelectOperator(scanTables[i], selectionexpression.ExpressionTouse);
				RightOperator = selectclause;
				// add expression which is already used in the usedExpression list so we dont
				// need to apply it again
				usedExpressionList.addAll(selectionexpression.UseExpressionList);
			} else {
				RightOperator = scanTables[i];
			}

			//////////////// Creation for Join Operator //////////////////////////

			// create attribute list which consist of join attribute from both tables which
			// is used to filter where clause
			List<String> newAtrributeList = new ArrayList<String>();
			newAtrributeList.addAll(LeftOperator.SchemaAttributes);
			newAtrributeList.addAll(RightOperator.SchemaAttributes);

			// select join clause by using the attribute list passed
			selectionexpression = new FilterSelectAndJoinExpression(newAtrributeList, usedExpressionList);
			if (whereclause != null) {
				whereclause.accept(selectionexpression);
			}
			if (selectionexpression.ExpressionTouse != null) {
				usedExpressionList.addAll(selectionexpression.UseExpressionList);
			}

			// join operator is created by passing Scan/select operator and expression used
			// for join
			JoinOperator join = new JoinOperator(LeftOperator, RightOperator, selectionexpression.ExpressionTouse);
			LeftOperator = join;
		}

		return LeftOperator;

	}

	/**
	 * jsqlParser PlainSelect.getSelectBody() is used to extract the select
	 * conditions in the form of "S.A" where S is the Alias for table and A is
	 * schema attribute
	 * 
	 * @return This returns a list of columns for sort operation
	 */
	public List<OrderByElement> GetSortOperatorCondition() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
		List<OrderByElement> orderByElements = plain.getOrderByElements();
		return orderByElements;
	}

	/**
	 * JSQLParser getDistinct() is used to check if query contains DISTINCT keyword
	 * 
	 * @return return boolean, true if DISTINCT keyword is used otherwise false
	 */
	public boolean HaveDistint() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
		if (plain.getDistinct() != null) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method calls the dump method of resultant operator which iteratively
	 * call resultant operator getNextTuple() until the next tuple is null (no more
	 * output) and writes each tuple to Output substring.This is sometimes called
	 * “pulling tuples”. The output substring will be then written to the output
	 * file
	 * 
	 * @param outputFilePath : output csv file path
	 */
	public void ExcecuteAndDump(String outputFilePath) {
		File f = new File(outputFilePath);
		f.delete();

		try (PrintWriter writer = new PrintWriter(new File(outputFilePath))) {

			ResultOperator.dump();
			writer.write(ResultOperator.Output.toString());

			System.out.println("The Output results is store in the file: ");
			System.out.println("done!");

		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}

}
