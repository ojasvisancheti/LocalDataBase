package ed.inf.adbs.lightdb.Operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import ed.inf.adbs.lightdb.Catalog;
import ed.inf.adbs.lightdb.Catalogs;
import ed.inf.adbs.lightdb.Tuple;

/**
 * ScanOperator extends Operator abstract class. Every instance of ScanOperator
 * knows which base table it is scanning and also if any alias associated with
 * it. Upon initialization, it opens a file using the catalog, scan on the
 * appropriate data file; when getNextTuple() is called, it reads the next line
 * from the file and returns the next tuple.
 *
 */
public class ScanOperator extends Operator {

	public String TableName = " ";

	public String Alias = " ";

	BufferedReader csvReader = null;

	public Catalog Tablecatalog = null;

	/**
	 * operator takes an argument as tableName and alias, using table name and table
	 * catalog open a file associated with it. It also creates an SchemaAttribute
	 * list with respect to a table(example. R.A, R.B, R.C) that makes the
	 * processing easy and manageable.
	 * 
	 * @param tableName :TableName to scan
	 * @param alais:    alias Name if any Alias is present for a table in the query
	 */
	public ScanOperator(String tableName, String alais) throws FileNotFoundException {
		TableName = tableName;
		Alias = alais;
		if (alais == null) {

			Alias = TableName;
		}

		Catalogs catalogs = Catalogs.getInstance();
		Tablecatalog = catalogs.GetCatalog(tableName);
		CreateAttributeWithAlais();
		csvReader = new BufferedReader(new FileReader(Tablecatalog.TablePath));
	}

	/**
	 * When getNextTuple() is called, it reads the next line from the csvReader
	 * (file open) and returns the next tuple. We can call getNextTuple() repeatedly
	 * to scan next line in form of tuple. If the file still has some available
	 * line, it will return the next line in form of tuple, otherwise it will return
	 * null.
	 * 
	 * @return Single tuple object or null if no object is available
	 */
	@Override
	public Tuple getNextTuple() {
		int[] tuplelist = null;
		Tuple tuple = null;
		String row = null;
		try {
			row = csvReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (row != null && row.isEmpty() == false) {
			tuplelist = Arrays.asList(row.split(",")).stream().mapToInt(Integer::parseInt).toArray();
			tuple = new Tuple(SchemaAttributes, tuplelist);
			return tuple;
		}

		try {
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tuple;
	}

	/**
	 * This method tells the operator to reset its state and start returning its
	 * output again from the beginning; that is, after calling reset() on an
	 * operator, a subsequent call to getNextTuple() will return the first line of
	 * file in form of tuple. This functionality is useful if you need to process an
	 * operator’s output multiple times.
	 */
	@Override
	public void reset() {
		try {
			csvReader.close();
			csvReader = new BufferedReader(new FileReader(Tablecatalog.TablePath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * This method create a schema attribute list depending on table the scanner
	 * operator is for, example if scan has TableName "Reserve" with Alais "R" and
	 * table has schema as "A, B, C". Then we will have the SchemaAttributes as
	 * R.A,R.B R.C. Each operator knowing which schema attribute its dealing with
	 * make the processing easy and manageable.
	 */
	private void CreateAttributeWithAlais() {
		SchemaAttributes = new ArrayList<String>();
		for (int i = 0; i < Tablecatalog.SchemaAttributes.size(); i++) {
			SchemaAttributes.add(Alias + "." + Tablecatalog.SchemaAttributes.get(i));
		}
	}

}
