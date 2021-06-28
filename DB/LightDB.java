package ed.inf.adbs.lightdb;

import java.io.IOException;

import net.sf.jsqlparser.JSQLParserException;

/**
 * Lightweight in-memory database system, it takes in a database (a set of files
 * with data) and an input file containing one SQL query. The program will
 * process and evaluate the SQL query on the database, create query plan,
 * execute plan and write the query result in the specified output file.
 */
public class LightDB {

	public static void main(String[] args) throws IOException, JSQLParserException {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		Catalogs catalogs = Catalogs.getInstance();
		catalogs.AddCatalogs(databaseDir);
		BuildQuery sqlQuery = new BuildQuery(inputFile);
		sqlQuery.ExcecuteAndDump(outputFile);
	}

}
