package ed.inf.adbs.lightdb.Operators;

import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.schema.Column;

/**
 * ProjectionOperator extends Operator abstract class. Every instance of
 * ProjectionOperator has Operator as its "child" and list of columns
 * "coloumstoProject" as columns to project from SQL query. During evaluation,
 * the ProjectionOperator's getNextTuple() method will grabs the next tuple from
 * its child. It extracts only the desired attributes using "coloumstoProject" ,
 * makes them into a new tuple and returns that.
 */
public class ProjectionOperator extends Operator {

	Operator child = null;
	List<Column> coloums = new ArrayList<Column>();

	/**
	 * @param operator         : Operator as a child
	 * @param coloumstoProject : columns to project
	 */
	public ProjectionOperator(Operator operator, List<Column> coloumstoProject) {

		child = operator;
		coloums = coloumstoProject;
		if (coloumstoProject != null) {
			for (Column object : coloumstoProject) {
				SchemaAttributes.add(object != null ? object.toString() : "");
			}
		}
	}

	/**
	 * This method grabs the next tuple from its child. It extracts only the desired
	 * attributes by using "coloumstoProject" , makes them into a new tuple and
	 * returns that. We can call getNextTuple() repeatedly to get next tuple,
	 * otherwise it will return null if there will be no tuple to pass.
	 * 
	 * @return Single tuple object or null if no object is available
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = child.getNextTuple();
		Tuple newTuple = new Tuple();
		while (tuple != null) {

			for (int i = 0; i < coloums.size(); i++) {
				String coloumName = coloums.get(i).getFullyQualifiedName().toString();
				if (tuple.TupleElements.containsKey(coloumName)) {
					newTuple.TupleElements.put(coloumName, tuple.TupleElements.get(coloumName));
				}

			}
			return newTuple;
		}

		return null;
	}

	/**
	 * This method tells the operator to reset its child state and start returning
	 * its output again from the beginning; that is, after calling reset() on an
	 * operator, a subsequent call to getNextTuple() will return the tuple. This
	 * functionality is useful if you need to process an operator’s output multiple
	 * times.
	 */
	@Override
	public void reset() {
		child.reset();

	}

}
