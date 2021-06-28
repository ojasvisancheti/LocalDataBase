package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/**
 * Catalog keep track of information about table data. where a file for a given
 * table is located, what is the schema of table along with table name.
 */

public class Catalog {

	public String TableName;
	public ArrayList<String> SchemaAttributes = new ArrayList<String>();
	public String TablePath;

}
