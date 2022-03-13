package com.huntsman.endpoints;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountEndPoints {
	// Declare the directory path of the AD and non AD registers as a constant.
	static String DIR_PATH = "C:\\Users\\priya\\git\\repository\\HuntsmanUniqueEndPoints\\WebContent\\resourcefiles";
	// Constants required to check against the 'uniquetable'.
	static String COL_NAME = "columnName";
	static String VAL = "value";
	static String CHK_SQL = "select * from uniquetable where exists( select * from uniquetable where " + COL_NAME + " = " + VAL +")";
	// Constant required for end point equality check. The equality order is determined by the field position. In this case - The first array element "domain" is checked first.  
	static String[] uniquetablefields = { "domain", "host", "ip", "azureid" };
	// Connection DB_URL
	static String CONNECTION_DB_URL = "jdbc:sqlserver://SURFACEPRO4;integratedSecurity=true;database=master;encrypt=true;trustServerCertificate=true;";
	
	public static void main(String args[]) {
		Runtime gfg = Runtime.getRuntime();
		System.out.println(" Before Main - "+gfg.freeMemory()+" :total: "+gfg.totalMemory());
		Connection connection = null;
		Statement statement = null;
		try {
			
			connection = DriverManager.getConnection(CONNECTION_DB_URL);
			statement = connection.createStatement();
			
			File directoryPath = new File(DIR_PATH);
			// List of all files from this directory.
			String files[] = directoryPath.list();
			// Loop through the register one-by-one. Assuming the register is a CSV file containing the end-points.
			for (int i = 0; i < files.length; i++) {
				readAndProcessRegister(connection, statement, files[i], i);
			}
			
			int countUniqueEndPoints = countUniqueEndpoints(connection,statement);
			System.out.println("The number of unique end points is : "+ countUniqueEndPoints);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
				statement = null;
				connection = null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(" After main - "+gfg.freeMemory());
		}
	}
	
	/**
	 * Method to read and process the register entries.
	 * 
	 * @param fileName
	 * @param fileCounter
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws Exception
	 * 
	 */
	private static void readAndProcessRegister(Connection connection, Statement statement, String fileName, int fileCounter) throws FileNotFoundException, IOException, SQLException, ClassNotFoundException{
		
		FileReader fileReader = new FileReader(DIR_PATH + "\\" + fileName);
		BufferedReader br = new BufferedReader(fileReader);
		// Read the register and process the end point records
		try {
			String line;
			int rowcount = 1;
			String[] fieldNames = null;
			
			List<String> insertSQLsForRegister = new ArrayList<String>();
			List<String> insertSQLsForUniqueTable = new ArrayList<String>();
			
			// Loop through each line in the register using buffering to improve I/O performance.
			while ((line = br.readLine()) != null) {
				// First line or record in the input CSV register should be the field names. Store it in a separate array.
				if (rowcount == 1) {
					fieldNames = line.split(";");
				} else {
					boolean canInsertToUniqueTable = false;
					// Validate if the number of end point values matches the number of field names in the header.
					String[] splits = line.split(";");
					if (fieldNames.length != splits.length) {
						throw new IOException("Invalid Input File !");
					}
					String[] fieldValues = new String[splits.length];
					for(int i=0;i<splits.length;i++) {
						fieldValues[i] = "\'"+ splits[i] +"\'" ;
					}
					Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");	
					// Construct SQL insert statement (sql string) using the CSV field names to insert the CSV end point record  to the respective relational database table. 
					// A bulk update to the respective relational database table will be performed later once all the sql insert statements are constructed for the whole register/file.
					String sql = "insert into " + fileName.replace(".csv", "") + " ("
							+ String.join(",", Arrays.asList(fieldNames)) + ") values ( "
							+ String.join(",", Arrays.asList(fieldValues)) + ")";
					
					insertSQLsForRegister.add(sql);
					// Insert into the 'uniquetable' after checking for duplicates.
					// Ignore to check for duplicates in the 'uniquetable' while processing the first register.
					if (fileCounter > 0) {
						for (int k = 0; k < uniquetablefields.length; k++) {
							int index = Arrays.binarySearch(fieldNames, uniquetablefields[k]);
							if (index >= 0) {
								String checksql = CHK_SQL.replace(COL_NAME, uniquetablefields[k]);
								checksql = checksql.replace(VAL, "\'"+ splits[index] +"\'" );
								
								// execute checksql
								statement.execute(checksql);
							    ResultSet resultSet = statement.getResultSet();
							    
								// if count is not 1 then insert to the 'uniquetable'. add unique entries to this table.
								if(!resultSet.next()) {
									canInsertToUniqueTable = true;
									break;
								}
							}
						}
					}
					// when processing the first register always insert to the uniquetable; for other subsequent registers check if the endpoint already exists.
					if (fileCounter == 0 || canInsertToUniqueTable) {
						String uniqueTableinsertSQL = "insert into uniquetable ("+String.join(",", Arrays.asList(fieldNames)) + ") values ( "+ String.join(",", Arrays.asList(fieldValues)) + ")";
						insertSQLsForUniqueTable.add(uniqueTableinsertSQL);
					}
				}
				rowcount++;
			}
			
			if(insertSQLsForRegister != null && insertSQLsForRegister.size()>0) {
				insertIntoCorrespondingDestinationRegister(connection, statement, insertSQLsForRegister);
			}
			if(insertSQLsForUniqueTable != null && insertSQLsForUniqueTable.size()>0) {
				insertIntoCorrespondingDestinationRegister(connection, statement, insertSQLsForUniqueTable);
			}
			
		} finally {
			fileReader.close();
			br.close();
		}
	}
	/**
	 * Utility Method to insert into tables using batch.
	 * 
	 * @param connection
	 * @param statement
	 * @param insertSqls
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * 
	 * 
	 */
	private static void insertIntoCorrespondingDestinationRegister(Connection connection, Statement statement, List<String> insertSqls) throws ClassNotFoundException, SQLException {
		for (String query : insertSqls) {
			statement.addBatch(query);
		}
		statement.executeBatch();	
	}
	
	/**
	 * Utility Method to count the rows in the uniquetable. 
	 * 
	 * @param connection
	 * @param statement
	 * @return
	 * @throws SQLException
	 */
	private static int countUniqueEndpoints(Connection connection, Statement statement) throws SQLException {
		ResultSet rs = statement.executeQuery("select count(*) as count from uniquetable");
		while(rs.next()) {
			return rs.getInt("count");
		}
		return 0;
	}
}
