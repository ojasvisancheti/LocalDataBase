package ed.inf.adbs.lightdb.Helper;

import java.util.Comparator;
import java.util.List;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * This class is a custom comparator for Tuple, the comparison is performed
 * based on the column name passed as "coloumNamestoCompare"
 */
public class TupleComparator implements Comparator<Tuple> {
	List<OrderByElement> coloumNamestoCompare = null;

	public TupleComparator(List<OrderByElement> orderbyelements) {

		coloumNamestoCompare = orderbyelements;
	}

	/**
	 * This overrides the compare method of Comparator. it will compare with first
	 * "coloumNamestoCompare" if there is a tie will compare with other columns if
	 * exists
	 */
	@Override
	public int compare(Tuple t1, Tuple t2) {
		int outputvalue = 0;
		if (coloumNamestoCompare != null) {
			for (int i = 0; i < coloumNamestoCompare.size(); i++) {
				outputvalue = t1.TupleElements.get(coloumNamestoCompare.get(i).toString())
						.compareTo(t2.TupleElements.get(coloumNamestoCompare.get(i).toString()));
				if (outputvalue == 0 && i < coloumNamestoCompare.size()) {
					continue;
				} else {
					return outputvalue;
				}
			}
		}
		return outputvalue;
	}

}