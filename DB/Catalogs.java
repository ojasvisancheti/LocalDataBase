package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Catalogs is a singleton class. It keep track of list of table catalog.
 */
public class Catalogs {

	// static variable single_instance of type Singleton
	private static Catalogs single_instance = null;

	private ArrayList<Catalog> cataloglist = new ArrayList<Catalog>();

	// private constructor restricted to this class itself
	private Catalogs() {

	}

	// static method to create instance of Singleton class
	public static Catalogs getInstance() {
		if (single_instance == null)
			single_instance = new Catalogs();

		return single_instance;
	}

	/**
	 * This method takes database directory path and create a catalog for each table
	 * The names schema.txt and data used are hard-coded and must exist in a valid
	 * database directory.
	 * 
	 * @param dbDirectory: database directory path
	 * @throws IOException
	 */
	public void AddCatalogs(String dbDirectory) throws IOException {
		String schemaPath = dbDirectory + File.separator + "schema.txt";

		try (BufferedReader dbReader = new BufferedReader(new FileReader(schemaPath))) {
			String row;

			while ((row = dbReader.readLine()) != null) {
				Catalog catalog = new Catalog();
				String[] data = row.split(" ");
				for (int i = 0; i < data.length; i++) {

					if (i == 0) {
						catalog.TableName = data[i];
					} else {
						catalog.SchemaAttributes.add(data[i]);
					}
				}

				catalog.TablePath = dbDirectory + File.separator + "data" + File.separator + catalog.TableName + ".csv";
				cataloglist.add(catalog);

			}
		}

	}

	/**
	 * This method takes the name of the table and provide catalog of that
	 * particular table
	 * 
	 * @param tableName : Name of the table
	 * @return Catalog for a table
	 */
	public Catalog GetCatalog(String tableName) {
		for (Integer i = 0; i < cataloglist.size(); i++) {
			if (cataloglist.get(i).TableName.equals(tableName)) {
				return cataloglist.get(i);
			}
		}

		return null;
	}

	/**
	 * @return Catalog for a all tables collected
	 */
	public ArrayList<Catalog> GetAllCatalog() {
		return cataloglist;
	}
}
