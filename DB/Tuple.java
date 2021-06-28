package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tuple keep tracks of tuple values in the Map form. Map contains key as schema
 * attribute and value as tuple attribute.
 */
public class Tuple {

	public Map<String, Integer> TupleElements = new LinkedHashMap<String, Integer>();

	private int hashCode = 0;

	/**
	 * @param attributes : schema attributes
	 * @param tuple      : tuple attributes
	 */
	public Tuple(ArrayList<String> attributes, int[] tuple) {

		for (int i = 0; i < tuple.length; i++) {
			TupleElements.put(attributes.get(i).toString(), tuple[i]);
		}
	}

	public Tuple() {
		TupleElements = new LinkedHashMap<String, Integer>();
	}

	/**
	 * This method overrides equal operator to compare two tuples here each value of
	 * tuple is compared to get the result. This method used by
	 * DuplicateEliminationOperator
	 */
	@Override
	public boolean equals(Object obj) {
		Tuple TupleToCompare = (Tuple) obj;
		List<String> keys = GetTupleAttributes();
		if (keys != null) {
			for (int i = 0; i < keys.size(); i++) {

				if (!(TupleToCompare.TupleElements.get(keys.get(i)).equals(TupleElements.get(keys.get(i))))) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * it returns hash code zero
	 */
	@Override
	public int hashCode() {
		return this.hashCode;
	}

	/**
	 * @return it returns the schema attribute list
	 */
	private List<String> GetTupleAttributes() {
		if (TupleElements != null) {
			return new ArrayList<>(TupleElements.keySet());
		}

		return null;

	}

}
