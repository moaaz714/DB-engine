import java.util.Iterator;
import java.util.Vector;
import java.util.List;
import java.util.RandomAccess;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
//Rabena yostor
public class DBApp implements Serializable {
	Table[] tableList = {};
	IndexPair[] indexPairs = {}; // List of Pairs (IndexName,bplustree)
	static int maxEntriesPerPage;
	static String appPath;
	static String metaData;
	// TODO the idea of forcing only 1 app being created must be thought about
	// thoroughly
	// we also need to handle not creating the csv multiple times
	public DBApp() throws DBAppException {
		AppConfigClass33 acc=new AppConfigClass33();
		
		String appCreated=acc.getAppCreatedFromConfig();
		appPath=acc.getAppPathFromConfig();
		metaData=acc.getMetaDataFromConfig();
		DBApp.maxEntriesPerPage=Integer.parseInt(acc.getmMaxEntriesPerPageFromConfig());
		
		if (appCreated.compareTo("true")==0) {
			System.out.println("the app already exists. Instance set to the existing app");
			DBApp db = DBApp.deserializeAll();
			this.tableList = db.tableList;
			this.indexPairs = db.indexPairs;
		} else {

			acc.setAppCreatedInConfig("true");
			appPath=acc.getAppPathFromConfig();
			metaData=acc.getMetaDataFromConfig();
			DBApp.maxEntriesPerPage=Integer.parseInt(acc.getmMaxEntriesPerPageFromConfig());
			
			String directoryPath = "D:\\Semester 6\\DB\\";
			String fileName = "metadata.csv";
			
			try {
				// Create a File object representing the file
				File file = new File(directoryPath, fileName);

				// Create the file
				if (file.createNewFile()) {
					System.out.println("MetaData created successfully at: " + file.getAbsolutePath());
				} else {
					System.out.println("MetaData already exists.");
				}
			} catch (IOException e) {
				System.out.println("An error occurred while creating metadata: " + e.getMessage());
				e.printStackTrace();
			}

			try {
				// Create a File object representing the file
				File file = new File(directoryPath, "DBApp.class");

				// Create the file
				if (file.createNewFile()) {
					System.out.println("DBApp.class created successfully at: " + file.getAbsolutePath());
				} else {
					System.out.println("DBApp.class already exists.");
				}
			} catch (IOException e) {
				System.out.println("An error occurred while creating metadata: " + e.getMessage());
				e.printStackTrace();
			}

			this.serialize();
		}
	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

	}

	public void shutdown() {

		this.serialize();
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		String directoryPath = metaData;
		BufferedReader br = new BufferedReader(new FileReader(directoryPath));
		String line = br.readLine();

		while (line != null) {
			String[] colInfo = line.split(",");

			if (colInfo[0].equals(strTableName)) {
				// found a table with matching name
				br.close();
				throw new DBAppException("A table already exists with the specified name");
			}
			line = br.readLine();
		}
		br.close();

		Table table = new Table(strTableName, htblColNameType.size());
		addTable(table);

		Enumeration<String> keys = htblColNameType.keys();

		try {
			FileWriter fileWriter = new FileWriter(metaData, true);

			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

			PrintWriter printWriter = new PrintWriter(bufferedWriter);

			while (keys.hasMoreElements()) {
				String key = keys.nextElement();
				String value = htblColNameType.get(key);
				String clustKey = "False";
				if (strClusteringKeyColumn == key) {
					clustKey = "True";
				}
				String input = strTableName + "," + key + "," + value + "," + clustKey + "," + null + "," + null;
				printWriter.println(input);
			}

			printWriter.close();

		} catch (IOException e) {
			System.out.println("An error occurred: " + e.getMessage());
			e.printStackTrace();
		}

	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) throws Exception {
		int tablePosition = findMetaDataRow(strTableName);
		EditMetaData(strTableName, strColName, strIndexName, tablePosition);

		ArrayList<String> columnNames = getColumnNamesFromCsv(strTableName);
		int columnPosition = findColumnPosition(columnNames, strColName);
		bplustree bTree = new bplustree(4);

		Table table = tableList[findTablePosition(strTableName)];

		for (Page page : table.pageList) {
			Vector<Tuple> tuples = page.deserializeAll();
			for (int i =0;i<tuples.size();i++) {
				Object value = tuples.get(i).values.get(columnPosition);
				String pageLocation =(page.pageName+","+i);
				bTree.insert(value, pageLocation);
			}
		}
		IndexPair indexPair = new IndexPair(strIndexName, bTree);
		addIndexPair(indexPair);
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {

		// TODO MAZ

		// we need to:
		// 1- check that the entry is all compatible (DONE)/
		// check that the number of columns in hash is the same as the number of
		// columns from csv(DONE)
		// check that all column names have an entry associated with them (DONE)
		// also handle wrong table name (DONE)(TESTED) -handled by calling
		// getPrimaryKeyPosition early -

		// 2- order the values such that tupleArray is in the correct
		// order(DONE)

		// 3- make sure the primary key value is not repeated (DONE)
		// -this is done when searching using the helper Page.findInsertionPosition as
		// it throws an exception -->
		// --> as well as the appropriate message when it finds a matching clust key
		// value when it's binary searching
		// for the insertion position-

		// 4-check if there are no pages, create page1

		// 5-if pages exist, find the position to insert the tuple

		// 6- shift and insert

		// 7- insert into B+
		// use findColIndex(String strTableName,String colName)
		// use findIndexPair(int low, int high, String strIndex)
		// pair.bplustree.insert(Object , pageName,rowNumber )

		int primaryKeyPosition = getPrimaryKeyPosition(strTableName); // i made the call to this method early
																		// to handle the case of calling insert with
																		// wrong table name

		Tuple tuple;
		ArrayList<Object> tupleArray = new ArrayList(); // this array will be used to initialize the tuple when
														// it contains the valuses in the correct order

		ArrayList<String> columnNames = this.getColumnNamesFromCsv(strTableName); // this contains all column
																					// names in the correct order

		boolean[] flags = new boolean[columnNames.size()]; // this will be used to make sure every column
															// name has a value associated with it in the hashtable

		boolean valueMismatch = false;
		Page currentPage;
		boolean thereExistsAnIndex = false;
		String clustringKeyName;

		if (htblColNameValue.size() != columnNames.size()) {
			throw new DBAppException("number of columns in hashtable does not match"
					+ " the number of columns associated with the table");
		}

		// the next loop validates each key value pair from csv

//TODO this loop as well as similar ones may face difficulties when inserting using a raw hashtable (unspecified)
// however , this is solved using the following lines
//for (Object entryObj : htblColNameValue.entrySet()) {
//Map.Entry<String, Object> entry = (Map.Entry<String, Object>) entryObj;		

		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (!validateFromCsv(strTableName, value, key)) // found an invalid
															// entry
			{
				throw new DBAppException("found an invalid entry");
			}
		}

		// this loop will initialise the flags bool[] such that it has true
		// wherever a column name is found in both columnNames and the hashtable ,
		// we will then loop over the flags in order to make sure they are all = true
		// which means they all column names from the csv have a matching column name
		// associated with it from the hash

		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
			String key = entry.getKey();
			for (int i = 0; i < columnNames.size(); i++) {

				if (columnNames.get(i).compareTo(key) == 0) {
					flags[i] = true;
				}
			}
		}

		// looping over flags to make sure it is all true
		for (int i = 0; i < flags.length; i++) {

			if (!flags[i]) {
				throw new DBAppException(
						"the column at position " + i + " does not have an column name associated with it in the hash");
			}
		}

		// for every column name in the correct order, loop over the already checked
		// hashtable column names,
		// when a match is found, insert the value associated with said column name into
		// the tuple array which
		// will be used to initialize the tuple
		for (int i = 0; i < columnNames.size(); i++) {
			String columnName = columnNames.get(i);
			for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
				String key = entry.getKey();
				if (columnName.compareTo(key) == 0) {
					tupleArray.add(entry.getValue());
				}
			}
		}

		tuple = new Tuple(tupleArray); // This tuple is initialized correctly
										// and in the correct order (same as
										// csv)

		// find clusterring key position (column 3 msln) so that you can compare
		// it to
		// all other clusterring keys from all pages
		// to make sure it is not repeated
		Table table = findTable(strTableName);

		if (table.pageList.length == 0) {
			// create first page
			currentPage = table.createpage(maxEntriesPerPage);
		}

		
		Object primaryKey = tuple.values.get(primaryKeyPosition);
		int targetPagePosition = table.pageChoiceHelper(primaryKey, 0, table.pageList.length - 1);

		// pageChoiceHelper will return a non existing page after the last if the entry
		// is larger than all previous
		// entries. In that case, we check for space in the last page , if none found ,
		// we create a page
		if (targetPagePosition == table.pageList.length) {
			// the clusterring key being inserted is the largest yet
			Page lastPage = table.pageList[table.pageList.length - 1];
			if (lastPage.isFull()) {
				// creating a new page to be able to insert after all old entries without
				// shifting them
				// no need to adjust targetPagePosition
				table.createpage(maxEntriesPerPage);

			} else {
				// i can insert in the last page so i will set targetPagePosition to the last
				// page
				targetPagePosition = targetPagePosition - 1;
			}

		}

		Page targetPage = table.pageList[targetPagePosition];

		int insertionPositionWithinPage = targetPage.findInsertionPosition(primaryKey, strTableName);
//TODO ALL ABOVE THIS LINE IS REVISED(yarab) -however, need to test from the previous todo till this one-

		// the following loop sets which page from the page list is to be used as the
		// shifting page for the
		// initial recursive call of the method addEntryAtPosition
		// thats the first non full page starting from and to the right of the
		// targetPage
		int shiftingPageInitialPosition = table.pageList.length - 1;
		for (int i = targetPagePosition; i <= table.pageList.length - 1; i++) {

			Vector<Tuple> tempVector = table.pageList[i].deserializeAll();
			if (tempVector.capacity() > tempVector.size()) {
				// found the page that is not full
				shiftingPageInitialPosition = i;
				break;

			}
		}
		
		table.addEntryAtPosition(targetPagePosition, shiftingPageInitialPosition, tuple, insertionPositionWithinPage,primaryKeyPosition);

		// use findColIndex(String strTableName,String colName)
		// use findIndexPair(int low, int high, String strIndex)
		// pair.bplustree.insert(Object , pageName,rowNumber)
		String pageNameCommaRow = targetPage.pageName + "," + insertionPositionWithinPage;
		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			String indexName = findColIndex(strTableName, key);
			if (indexName.compareTo("null") != 0) {
				// found an index name

				IndexPair indexPair = findIndexPair(0, this.indexPairs.length, indexName);

				indexPair.bplustree.insert(value, pageNameCommaRow);
			}

		}

		// note : anything below the following line is old and unrevised

		// ---------------------------------------------------------------------------------------------------------

//		for (int i = row; i < table.ColNum + row; i++) {
//			List<String[]> temp = readCSV();
//			String indexname = temp.get(i)[4].toString();
//
//			if (indexname.compareTo("null") != 0) {
//				// there exists an index
//				thereExistsAnIndex = true;
//				break;
//			}
//		}
//
//		if (thereExistsAnIndex)
//			insertIntoIndex(); // this needs to be implemented
//
//		throw new DBAppException("not implemented yet");
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue, 
			Hashtable<String, Object> htblColNameValue) 
			throws DBAppException, IOException {
		
		Table table = findTable(strTableName);//check if table is found in csv
		
		
		IndexPair indexpair= ClustKeyIndex(strTableName);
		if (indexpair !=null) {
			bplustree bplustree = indexpair.bplustree;
			
			
			ArrayList<String> columnarr = getColumnNamesFromCsv(strTableName);
	        String columnName = null;
	        Object newValue = null;
	        Enumeration<String> keys = htblColNameValue.keys(); // Get the Column Name and New Value separately from Hashtable
	        while (keys.hasMoreElements()) {
	            columnName = keys.nextElement();
	            newValue = htblColNameValue.get(columnName);
	        }
	        if (columnName == null || newValue == null) {
	            throw new DBAppException("Identify Column's Name and New Value!");
	        }
	        boolean flag = validateFromCsv(strTableName, newValue, columnName);
	        if (!flag) {
	            throw new DBAppException("Invalid data type (key does not match dataType).");
	        }
	        
	        
			String type = clusterType(strTableName);
			Object clustKey=null;
			Vector<String> arr=null;
			String pagePosition;
			switch(type) {
				case "java.lang.Integer":
					int x = Integer.parseInt(strClusteringKeyValue);
					arr=bplustree.search(x);
					pagePosition=arr.get(0);
					break;
				case "java.lang.String":
					arr=bplustree.search(strClusteringKeyValue);
					pagePosition=arr.get(0);
					break;
				case "java.lang.Double":
					double y = Double.parseDouble(strClusteringKeyValue);
					arr=bplustree.search(y);
					pagePosition=arr.get(0);
					break;
			}
  
		}
		
		
		
		ArrayList<String> columnarr = getColumnNamesFromCsv(strTableName);
		String columnName = null;
		Object newValue = null;
		
		Enumeration<String> keys = htblColNameValue.keys(); //get the Column Name and New Value
															//separately from Hashtable
        while (keys.hasMoreElements()) {  
            columnName = keys.nextElement();
            newValue = htblColNameValue.get(columnName);
        }
		
        if(columnName==null || newValue==null) {
			throw new DBAppException("Identify Column's Name and New Value!");
		}
		
		boolean flag = validateFromCsv(strTableName,newValue,columnName);
		
		if(!flag)
			throw new DBAppException("invalid data type (key does not match dataType)");
		
		
		
		Page[] tablePages = table.pageList;
		String clusteringKey = findClusteringKeyName(strTableName);
		String type = clusterType(strTableName);
		int pagePosition = -1;
		
		switch(type) {
			case "java.lang.Integer":
				int x = Integer.parseInt(strClusteringKeyValue);
				pagePosition = table.pageChoiceHelper(x, 0, table.pageList.length - 1);
				break;
			case "java.lang.String":
				pagePosition = table.pageChoiceHelper(strClusteringKeyValue, 0, table.pageList.length - 1);
				break;
			case "java.lang.Double":
				double y = Double.parseDouble(strClusteringKeyValue);
				pagePosition = table.pageChoiceHelper(y, 0, table.pageList.length - 1);
				break;
		}
		if(pagePosition==-1) {
			throw new DBAppException("invalid Data Type.");
		}else if(pagePosition==tablePages.length) {
			System.out.print("0 Rows Affected!");
			return;
		}
			
		Page page = tablePages[pagePosition];
		int rowinPage = -1;
		
		switch(type) {
		case "java.lang.Integer":
			int x = Integer.parseInt(strClusteringKeyValue);
			rowinPage= page.findInsertionPosition(x,strTableName);
			break;
		case "java.lang.String":
			rowinPage = page.findInsertionPosition(strClusteringKeyValue,strTableName);
			break;
		case "java.lang.Double":
			double y = Double.parseDouble(strClusteringKeyValue);
			rowinPage = page.findInsertionPosition(y,strTableName);
			break;
		}
		
		if(rowinPage==-1) {
			throw new DBAppException("Invalid data Type.");
		}
		
		Vector<Tuple> pageTuples = page.deserializeAll();
		int columnPosition = findColumnPosition(columnarr,columnName);

		pageTuples.get(rowinPage).values.remove(columnPosition);
		pageTuples.get(rowinPage).values.add(columnPosition, newValue);
		
		
		 page.serialize(pageTuples);
		
		//System.out.println("UPDATED "+page.deserializeAll().get(rowinPage).values.get(columnPosition));
		
			
}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together
	public void deleteFromTable(String strTableName,Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		ArrayList<String> targetColumns = new ArrayList<String>();
		ArrayList<Object> targetValues = new ArrayList<Object>();
		
		if(htblColNameValue.size()==0) {
			for(Table table :tableList) {
				for(Page page:table.pageList) {
					File myObj = new File(page.pagePath);
					myObj.delete();
					table.removePage(page);
				}
			}
			return;
		}
		
		//Get columns and their values
		Enumeration<String> keys = htblColNameValue.keys();
		while (keys.hasMoreElements()) {
			String column=keys.nextElement();
			Object value=htblColNameValue.get(column);
			if(!this.validateFromCsv(strTableName,value,column)) {
				System.out.println("These columns don't belong to this table");
				return ;
			}
			targetColumns.add(column);
			targetValues.add(value);
		}
		String clustKey=this.findClusteringKeyName(strTableName);
		Hashtable tuples= new Hashtable();
		Table table = findTable(strTableName);
		ArrayList<String> tableColumns = this.getColumnNamesFromCsv(strTableName);
		
		int p=0;
		String indexName="null";
		
		ArrayList<IndexPair> indexPa = new ArrayList<IndexPair>();
		ArrayList<Integer> indexPos = new ArrayList<Integer>();
		for(int r=0;r<targetColumns.size();r++) {
			indexName = this.findColIndex(strTableName, targetColumns.get(r));
			if(!indexName.equals("null")) {
				indexPa.add(this.findIndexPair(0, this.indexPairs.length-1, indexName));
				indexPos.add(this.findColumnPosition(tableColumns, targetColumns.get(r)));
			}
		}
	
		
		while(p<targetColumns.size() && tuples.size()==0){
			for(p=p;p<targetColumns.size();p++) {
				indexName = this.findColIndex(strTableName, targetColumns.get(p));
				if(!indexName.equals("null")) {
					break;
				}
			}
			
			if(p==targetColumns.size()) {
				p=p-1;
			}
			
			if(!indexName.equals("null")) {
				IndexPair indexPair= this.findIndexPair(0, indexPairs.length-1, indexName);
				Vector<String> locations = (Vector<String>) indexPair.bplustree.search(targetValues.get(p));
				if(locations!=null) {
					for(String location : locations) {
						Page page = table.findPage(0, table.pageList.length-1, location.split(",")[0]);
						Tuple t = page.deserializeAll().get(Integer.parseInt(location.split(",")[1]));
						System.out.println(p);
						int row=this.findColumnPosition(tableColumns, targetColumns.get(p));
						if(compare(t.values.get(row),targetValues.get(p))==0){
							 tuples.put(t,location);
						 }
					}
				}
				
			}else {
				for(Page page : table.pageList) {
					Vector<Tuple> vector = page.deserializeAll();
					for(int i =0;i<vector.size();i++) {
						Tuple t = vector.get(i);
						int row=this.findColumnPosition(tableColumns, targetColumns.get(p));
						System.out.println(t.values.get(row)+" "+targetValues.get(p));
						if(compare(t.values.get(row),targetValues.get(p))==0){
							 tuples.put(t,page.pageName+","+i);
						 }
					}
				}	
			}
			p++;
		}
		
		if(tuples.size()==0) {
			System.out.println("nothing was needed to be deleted");
			return;
		}
		
		
		
		
		
		 for(int i=0;i<targetColumns.size();i++) {
			 int rowNum = this.findColumnPosition(tableColumns, targetColumns.get(i));
			 Enumeration<Tuple> tupleKeys = tuples.keys();
			 
			 while (tupleKeys.hasMoreElements()) {
				 Tuple tuple=tupleKeys.nextElement();
				 Object page=tuples.get(tuple);
				 if(compare(tuple.values.get(rowNum),targetValues.get(i))!=0){
					 tuples.remove(tuple);
				 }
			 }
			 
		 }
		 
		
		 
		 
		//To Delete the tuples
		Enumeration<Tuple> tupleKeys = tuples.keys();
		while (tupleKeys.hasMoreElements()) {
			Tuple key=tupleKeys.nextElement();
			//To remove from the Page
			String location=(String) tuples.get(key);
			for(int i =0;i<indexPa.size();i++) {
				indexPa.get(i).bplustree.delete(key.values.get(indexPos.get(i)));
			}
			
			String pageName = location.split(",")[0];
			int rowNum = Integer.parseInt(location.split(",")[1]);
			Page page =table.findPage(0, table.pageList.length-1, pageName);
			page.removeTuple(rowNum);
			if(page.isEmpty()) {
				File myObj = new File(page.pagePath);
				myObj.delete();
				table.removePage(page);
			}
			
			
			
			
			
			
		}
		
	}
	
public String clusterType(String strTableName) throws DBAppException {
		
		String csvFile = DBApp.metaData; // Path to CSV file
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			String line;

			// this loops over all lines
			while ((line = br.readLine()) != null) {
				// Splitting the line by comma to get individual fields
				String[] fields = line.split(",");

				// Process each field
				if (fields[0].equals(strTableName)) {
					if(fields[3].equals("True")) {
						
						return fields[2];
					}
					
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		throw new DBAppException("type not found");

		
		
	}
	
	
	public IndexPair ClustKeyIndex(String strTableName) throws DBAppException, IOException {
		IndexPair pair=null;
		String clustKeyName=this.findClusteringKeyName(strTableName);
		String indexName=this.findColIndex(strTableName, clustKeyName);
		
		if(indexName.equals("null")) {
			
			return null;
		}
		
		pair=this.findIndexPair(0, this.indexPairs.length-1, indexName);
		return pair;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException {
		ArrayList<String> targetColumns = new ArrayList<String>();
		ArrayList<String> operators = new ArrayList<String>();
		ArrayList<Object> targetValues = new ArrayList<Object>();
		
		
		
		String strTableName=arrSQLTerms[0]._strTableName;
		Hashtable htblColNameValue = new Hashtable();
		
		for(int i =0; i<arrSQLTerms.length;i++) {
			htblColNameValue.put(arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._objValue);
			operators.add(arrSQLTerms[i]._strOperator);
		}
		//Get columns and their values
		Enumeration<String> keys = htblColNameValue.keys();
		while (keys.hasMoreElements()) {
			String column=keys.nextElement();
			Object value=htblColNameValue.get(column);
			if(!this.validateFromCsv(strTableName,value,column)) {
				System.out.println("These columns don't belong to this table");
				return null;
			}
			targetColumns.add(column);
			targetValues.add(value);
		}
		String clustKey=this.findClusteringKeyName(strTableName);
		ArrayList<Tuple> tuples= new ArrayList<Tuple>();
		Table table = findTable(strTableName);
		ArrayList<String> tableColumns = this.getColumnNamesFromCsv(strTableName);
		
		int p=0;
		String indexName="null";
		
	
		
		while(p<targetColumns.size()){
			indexName = this.findColIndex(strTableName, targetColumns.get(p));
				
			
			if(!indexName.equals("null")) {
				IndexPair indexPair= this.findIndexPair(0, indexPairs.length-1, indexName);
				Vector<String> locations = (Vector<String>) indexPair.bplustree.search(targetValues.get(p));
				if(locations!=null) {
					for(String location : locations) {
						Page page = table.findPage(0, table.pageList.length-1, location.split(",")[0]);
						Tuple t = page.deserializeAll().get(Integer.parseInt(location.split(",")[1]));
						System.out.println(p);
						int row=this.findColumnPosition(tableColumns, targetColumns.get(p));
						if(operatorCompare(t.values.get(row),targetValues.get(p),operators.get(p))){
							if(p==0 || (p!=0 && strarrOperators[p-1].equals("OR"))){
								 tuples.add(t);
							}else {
								for(Tuple tuple : tuples) {
									if(this.tupleCompare(tuple, t, strarrOperators[p-1])) {
										tuples.add(tuple);
									}else {
										tuples.remove(tuple);
									}
								}
							}
						 }
					}
				}
				
			}else {
				for(Page page : table.pageList) {
					Vector<Tuple> vector = page.deserializeAll();
					for(int i =0;i<vector.size();i++) {
						Tuple t = vector.get(i);
						int row=this.findColumnPosition(tableColumns, targetColumns.get(p));
						if(operatorCompare(t.values.get(row),targetValues.get(p),operators.get(p))){
							if(p==0 || (p!=0 && strarrOperators[p-1].equals("OR"))){
								 tuples.add(t);
							}else {
								for(Tuple tuple : tuples) {
									if(this.tupleCompare(tuple, t, strarrOperators[p-1])) {
										tuples.add(t);
									}else {
										tuples.remove(t);
									}
								}
							}
						 }
					}
				}	
			}
			p++;
		}
		
		ArrayList<String> out = new ArrayList<String>();
		
		for(Tuple tt:tuples) {
			out.add(tt.toString());
		}
		Iterator itr =out.iterator();
		return itr;
		
	}

	

	public bplustree findBplusTree(String indexNames) {
		bplustree result = new bplustree(4);
		for (int i = 0; i < indexPairs.length; i++) {
			if (indexPairs[i].indexName.equals(indexNames)) {
				result = indexPairs[i].bplustree;
			}
		}
		return result;
	}

	public Page findPage(Table table, String pageName) {
		for (int i = 0; i < table.pageList.length; i++) {
			if (table.pageList[i].pageName.equals(pageName))
				return table.pageList[i];
		}
		return null;
	}

	// help not really just check it pls
	public ArrayList<Tuple> bPlusTreeArithmeticOperator(SQLTerm sqt, String bPlusTreeName, Page[] tablePages,
			int colPos) throws DBAppException {// typecasting revised
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		bplustree tree = findBplusTree(bPlusTreeName);
		if (sqt._strOperator == "=") {
			String location = String.valueOf(tree.search(sqt._objValue)); // assuming
			// object
			// returned
			// is a
			// string of
			// the form PageNo,Row (can it return multiple?
			String[] actualLocation = location.split(",");
			Page targetPage;
			for (int i = 0; i < (actualLocation.length) / 2; i++) {
				targetPage = findPage(findTable(sqt._strTableName), actualLocation[i]); // length
																						// or
																						// length
																						// -1
				Vector<Tuple> v = targetPage.deserializeAll();
				int row = Integer.parseInt(actualLocation[i + 1]);
				result.add(v.get(row));
			}
		} else {

			if (sqt._strOperator == ">") {
				int inc = 0;
				Object target = (tree.firstLeaf).dictionary[inc].key;
				ArrayList<Object> locations = tree.search(sqt._objValue, target);// start after object
																					// value
				for (int coun = 0; coun < locations.size(); coun++) {
					Object location = locations.get(coun);
					String loc = String.valueOf(location);
					String[] actualLocation = loc.split(",");
					Page targetPage;
					for (int i = 0; i < (actualLocation.length) / 2; i++) {
						targetPage = findPage(findTable(sqt._strTableName), actualLocation[i]); // length
																								// or
																								// length
																								// -1
						Vector<Tuple> v = targetPage.deserializeAll();
						int row = Integer.parseInt(actualLocation[i + 1]);
						if (sqt._objValue instanceof String) {
							String targ = v.get(row).toString();
							String[] tgt = targ.split(",");
							if (tgt[colPos].compareTo((String) (sqt._objValue)) != 0) {
								result.add(v.get(row));
							} else {
								if (sqt._objValue instanceof Integer) {
									Integer objValue = (int) sqt._objValue;
									int objVal = objValue.intValue();
									String targ1 = v.get(row).toString();
									String[] tgt1 = targ.split(",");
									int col = Integer.parseInt(tgt[colPos]);
									if (col != objVal) {
										result.add(v.get(row));
									}
								}
								if (sqt._objValue instanceof Double) {
									Double objValue = (Double) sqt._objValue;
									double objVal = objValue.doubleValue();
									String targ1 = v.get(row).toString();
									String[] tgt1 = targ.split(",");
									double col = Double.parseDouble(tgt[colPos]);
									if (col != objVal) {
										result.add(v.get(row));
									}
								}
							}
						}
					}
				}
			} else if (sqt._strOperator == ">=") {
				int inc = 0;
				Object target = (tree.firstLeaf).dictionary[inc].key;
				ArrayList<Object> locations = tree.search(sqt._objValue, target);
				for (int coun = 0; coun < locations.size(); coun++) {
					Object location = locations.get(coun);
					String loc = String.valueOf(location);
					String[] actualLocation = loc.split(",");
					Page targetPage;
					for (int i = 0; i < (actualLocation.length) / 2; i++) {
						targetPage = findPage(findTable(sqt._strTableName), actualLocation[i]);// length
																								// or
																								// length
																								// -1
						Vector<Tuple> v = targetPage.deserializeAll();
						int row = Integer.parseInt(actualLocation[i + 1]);
						result.add(v.get(row));
					}
				}
			} else if (sqt._strOperator == "<") {
				int inc = 0;
				Object target = (tree.firstLeaf).dictionary[inc].key;
				ArrayList<Object> locations = tree.search(target, sqt._objValue); // stop before obj value
				for (int coun = 0; coun < locations.size(); coun++) {
					Object location = locations.get(coun);
					String loc = String.valueOf(location);
					String[] actualLocation = loc.split(",");
					Page targetPage;
					for (int i = 0; i < (actualLocation.length) / 2; i++) {
						targetPage = findPage(findTable(sqt._strTableName), actualLocation[i]); // length
																								// or
																								// length
																								// -1
						Vector<Tuple> v = targetPage.deserializeAll();
						int row = Integer.parseInt(actualLocation[i + 1]);
						if (sqt._objValue instanceof String) {
							String targ = v.get(row).toString();
							String[] tgt = targ.split(",");
							if (tgt[colPos].compareTo((String) (sqt._objValue)) != 0) {
								result.add(v.get(row));
							} else {
								if (sqt._objValue instanceof Integer) {
									Integer objValue = (int) sqt._objValue;
									int objVal = objValue.intValue();
									String targ1 = v.get(row).toString();
									String[] tgt1 = targ.split(",");
									int col = Integer.parseInt(tgt[colPos]);
									if (col != objVal) {
										result.add(v.get(row));
									}
								}
								if (sqt._objValue instanceof Double) {
									Double objValue = (Double) sqt._objValue;
									double objVal = objValue.doubleValue();
									String targ1 = v.get(row).toString();
									String[] tgt1 = targ.split(",");
									double col = Double.parseDouble(tgt[colPos]);
									if (col != objVal) {
										result.add(v.get(row));
									}
								}
							}
						}
					}
				}

			} else if (sqt._strOperator == "<=") {
				int inc = 0;
				Object target = (tree.firstLeaf).dictionary[inc].key;
				ArrayList<Object> locations = tree.search(target, sqt._objValue);
				for (int coun = 0; coun < locations.size(); coun++) {
					Object location = locations.get(coun);
					String loc = String.valueOf(location);
					String[] actualLocation = loc.split(",");
					Page targetPage;
					for (int i = 0; i < (actualLocation.length) / 2; i++) {
						targetPage = findPage(findTable(sqt._strTableName), actualLocation[i]);// length
																								// or
																								// length
																								// -1
						Vector<Tuple> v = targetPage.deserializeAll();
						int row = Integer.parseInt(actualLocation[i + 1]);
						result.add(v.get(row));
					}
				}
			} else if (sqt._strOperator == "!=") {
				for (int i = 0; i < tablePages.length; i++) {
					result.addAll(linearArithmeticOperator(sqt, tablePages[i]));
				}
			}
		}
		return result;
	}

	public Object findLastLeafNode(LeafNode ln) {
		LeafNode current = ln;
		while (current.rightSibling != null) {
			current = ln.rightSibling;
		}
		return current;
	}

	// needs revision
	public ArrayList<Tuple> linearArithmeticOperator(SQLTerm sqt, Page page) throws DBAppException { // linear search
																										// //typecasting
																										// revised
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		Vector<Tuple> v = page.deserializeAll();
		ArrayList<String> keyNames = page.getColumnNamesFromCsv(sqt._strTableName);
		int colNum = 0;
		for (int i = 0; i < keyNames.size(); i++) {
			if (keyNames.get(i).compareTo(sqt._strColumnName) == 0) {
				colNum = i;
			}
		}
		for (int i = 0; i < v.size(); i++) {
			Tuple t = v.get(i);
			if (sqt._strOperator == ">") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) > 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check > objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check > objVal) {
						result.add(t);
					}
				}

			} else if (sqt._strOperator == "<") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) < 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check < objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check < objVal) {
						result.add(t);
					}
				}
			} else if (sqt._strOperator == ">=") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) >= 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check >= objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check >= objVal) {
						result.add(t);
					}
				}
			} else if (sqt._strOperator == "<=") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) <= 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check <= objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check <= objVal) {
						result.add(t);
					}
				}
			} else if (sqt._strOperator == "=") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) == 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check == objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check == objVal) {
						result.add(t);
					}
				}
			} else if (sqt._strOperator == "!=") {
				if (sqt._objValue instanceof String) {
					String check = (String) (t.values.get(colNum));
					String objVal = (String) sqt._objValue;
					if (check.compareTo(objVal) != 0) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Integer) {
					Integer objValue = (int) sqt._objValue;
					int objVal = objValue.intValue();
					Integer checkValue = (int) t.values.get(colNum);
					int check = checkValue.intValue();
					if (check != objVal) {
						result.add(t);
					}
				}
				if (sqt._objValue instanceof Double) {
					Double objValue = (Double) sqt._objValue;
					double objVal = objValue.doubleValue();
					Double checkValue = (Double) t.values.get(colNum);
					double check = checkValue.doubleValue();
					if (check != objVal) {
						result.add(t);
					}
				}
			} else {
				throw new DBAppException("Wrong operator (=,<,etc....)!");
			}

		}
		return result;
	}

	public ArrayList<Tuple> binarySearchArithmeticOperator(Table table, int primaryKeyPosition, SQLTerm sqt)
			throws DBAppException { // typecasting revised
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		Tuple tuple;

		// if you are searching for a number higher than the highest clust key value
		// pageChoiceHelper will return pageList.Length
		int targetPagePosition = table.pageChoiceHelper(sqt._objValue, 0, table.pageList.length - 1);

		if (targetPagePosition == table.pageList.length) {
			if (sqt._strOperator.equals("<") || sqt._strOperator.equals("<=") || sqt._strOperator.equals("!=")) {
				for (int i = 0; i < table.pageList.length; i++) {
					Vector<Tuple> tempVector = table.pageList[i].deserializeAll();
					for (int j = 0; j < tempVector.size(); j++) {
						result.add(tempVector.get(j));
					}

				}

			} else {
				return result;
			}

		}

		Page targetPage = table.pageList[targetPagePosition];
		int positionWithinPage = targetPage.findSelection(sqt._objValue, sqt._strTableName);

		Vector<Tuple> vector = targetPage.deserializeAll();

		// TODO fix switch statements other than String
		if (sqt._objValue instanceof String) {
			String objVal = (String) sqt._objValue;
			String foundTuple = (String) vector.get(positionWithinPage).values.get(primaryKeyPosition);
			switch (sqt._strOperator) {
			case ("="):
				if (objVal.compareTo(foundTuple) == 0) {
					result.add(vector.get(positionWithinPage));
				}
				break;
			case (">"):
				if (objVal.compareTo(foundTuple) == 0 || objVal.compareTo(foundTuple) > 0) {
					result.addAll(getRestOfVector(vector, positionWithinPage + 1));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				} else {
					result.addAll(getRestOfVector(vector, positionWithinPage));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				}
				break;
			case ("<"):
				if (objVal.compareTo(foundTuple) == 0 || objVal.compareTo(foundTuple) > 0) {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				} else {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage));

				}
				break;
			case (">="):
				if (objVal.compareTo(foundTuple) == 0 || objVal.compareTo(foundTuple) < 0)
					result.add(vector.get(positionWithinPage));

				result.addAll(getRestOfVector(vector, positionWithinPage + 1));
				if (targetPagePosition < table.pageList.length - 1) {
					result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
				}
				break;
			case ("<="):
				if (targetPagePosition > 0) {
					result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
				}
				result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				if (objVal.compareTo(foundTuple) <= 0)
					result.add(vector.get(positionWithinPage));

				break;
			case ("!="):
				result.addAll(linearArithmeticOperator(sqt, targetPage));
				break;
			}
		} else if (sqt._objValue instanceof Integer) {
			Integer objValue = (int) sqt._objValue;
			int objVal = objValue.intValue();
			Integer foundTupleValue = (int) vector.get(positionWithinPage).values.get(primaryKeyPosition);
			int foundTuple = foundTupleValue.intValue();

			switch (sqt._strOperator) {
			case ("="):
				if (objVal == foundTuple) {
					result.add(vector.get(positionWithinPage));
				}
				break;
			case (">"):
				if (objVal == foundTuple || objVal > foundTuple) {
					result.addAll(getRestOfVector(vector, positionWithinPage + 1));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				} else {
					result.addAll(getRestOfVector(vector, positionWithinPage));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				}
				break;
			case ("<"):
				if (objVal == foundTuple || objVal > foundTuple) {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				} else {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage));

				}
				break;
			case (">="):
				if (objVal == foundTuple || objVal < foundTuple)
					result.add(vector.get(positionWithinPage));

				result.addAll(getRestOfVector(vector, positionWithinPage + 1));
				if (targetPagePosition < table.pageList.length - 1) {
					result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
				}
				break;
			case ("<="):
				if (targetPagePosition > 0) {
					result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
				}
				result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				if (objVal <= foundTuple)
					result.add(vector.get(positionWithinPage));

				break;
			case ("!="):
				result.addAll(linearArithmeticOperator(sqt, targetPage));
				break;
			}
		} else { // Double
			Double objValue = (Double) sqt._objValue;
			double objVal = objValue.doubleValue();
			Double foundTupleValue = (Double) vector.get(positionWithinPage).values.get(primaryKeyPosition);
			double foundTuple = foundTupleValue.doubleValue();

			switch (sqt._strOperator) {
			case ("="):
				if (objVal == foundTuple) {
					result.add(vector.get(positionWithinPage));
				}
				break;
			case (">"):
				if (objVal == foundTuple || objVal > foundTuple) {
					result.addAll(getRestOfVector(vector, positionWithinPage + 1));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				} else {
					result.addAll(getRestOfVector(vector, positionWithinPage));
					if (targetPagePosition < table.pageList.length - 1) {
						result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
					}
				}
				break;
			case ("<"):
				if (objVal == foundTuple || objVal > foundTuple) {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				} else {
					if (targetPagePosition > 0) {
						result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
					}
					result.addAll(getBegOfVector(vector, positionWithinPage));

				}
				break;
			case (">="):
				if (objVal == foundTuple || objVal < foundTuple)
					result.add(vector.get(positionWithinPage));

				result.addAll(getRestOfVector(vector, positionWithinPage + 1));
				if (targetPagePosition < table.pageList.length - 1) {
					result.addAll(getRestOfTuplesInPages(table, targetPagePosition + 1));
				}
				break;
			case ("<="):
				if (targetPagePosition > 0) {
					result.addAll(getBegOfTuplesInPages(table, targetPagePosition - 1));
				}
				result.addAll(getBegOfVector(vector, positionWithinPage - 1));

				if (objVal <= foundTuple)
					result.add(vector.get(positionWithinPage));

				break;
			case ("!="):
				result.addAll(linearArithmeticOperator(sqt, targetPage));
				break;
			}
		}

		return result;
	}

	public ArrayList<Tuple> getRestOfTuplesInPages(Table table, int startPage) {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (int i = startPage; i < table.pageList.length; i++) {
			Vector<Tuple> vect = table.pageList[i].deserializeAll();
			result.addAll(getBegOfVector(vect, vect.size() - 1));
		}
		return result;
	}

	public ArrayList<Tuple> getBegOfTuplesInPages(Table table, int endPage) {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (int i = 0; i < endPage; i++) {
			Vector<Tuple> vect = table.pageList[i].deserializeAll();
			result.addAll(getBegOfVector(vect, vect.size() - 1));
		}
		return result;
	}

	public static ArrayList<Tuple> getRestOfVector(Vector<Tuple> originalVector, int startIndex) { // revised
		ArrayList<Tuple> resultList = new ArrayList<>();
		// Copy the elements from the original vector starting from startIndex
		for (int i = startIndex; i < originalVector.size(); i++) {
			// Create a tuple with the object and its index
			resultList.add(originalVector.get(i));
		}
		return resultList;
	}

	public static ArrayList<Tuple> getBegOfVector(Vector<Tuple> originalVector, int endIndex) { // revised
		ArrayList<Tuple> resultList = new ArrayList<>();
		// Copy the elements from the original vector from the beginning till
		// endIndex
		for (int i = 0; i <= endIndex; i++) {
			// Create a tuple with the object and its index
			resultList.add(originalVector.get(i));
		}
		return resultList;
	}

	// HELPER SECTION
	// --------------------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------------------
	
	
	public boolean tupleCompare(Tuple x, Tuple y, String operator) throws DBAppException {
		switch (operator) {
		case "AND":
			if(x.equals(y)) {
				return true;
			}
			break;
		case "XOR":
			if(!x.equals(y)) {
				return true;
			}
			break;
		default:
			throw new DBAppException("Operator not supported");
		}
		return false;
	}
	
	public boolean operatorCompare(Object x , Object y , String operator) throws DBAppException {
		switch (operator) {
		case ("="):
			if(this.compare(x, y)==0) {
				return true;
			}
			break;
		case (">"):
			if(this.compare(x, y)>0) {
				return true;
			}
			break;
		case ("<"):
			if(this.compare(x, y)<0) {
				return true;
			}
			break;
		case (">="):
			if(this.compare(x, y)>=0) {
				return true;
			}
			break;
		case ("<="):
			if(this.compare(x, y)<=0) {
				return true;
			}
			break;
		case ("!="):
			if(this.compare(x, y)!=0) {
				return true;
			}
			break;
		default:
			throw new DBAppException("One of the strOperator is not right");
		}
		return false;
	}


	
	public int findColumnPosition(ArrayList<String> columnNames, String strColName) {
		for (int i = 0; i < columnNames.size(); i++) {
			if (columnNames.get(i).equals(strColName)) {
				return i;
			}
		}
		return -1;
	}

	public ArrayList<String> getColumnNamesFromCsv(String tableName) {

		ArrayList<String> names = new ArrayList<>();
		String csvFile = metaData; // Path to your CSV file
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			String line;

			// this loops over all lines
			while ((line = br.readLine()) != null) {
				// Splitting the line by comma to get individual fields

				String[] fields = line.split(",");

				// Process each field
				if (fields[0].equals(tableName)) {
					names.add(fields[1]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return names;
	}

	// find which column within the columns of the index is the primary key
	// when you find column 0 of the desired table , start counting
	public static int getPrimaryKeyPosition(String strTableName) throws DBAppException {
		String csvFile = metaData; // Path to your CSV file
		int position = -1;
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				// Splitting the line by comma to get individual fields
				String[] fields = line.split(",");

				// Process each field
				if (fields[0].compareTo(strTableName) == 0) {
					position++;
					if (fields[3].compareTo("True") == 0) {
						return position;
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		if (position == -1) {
			throw new DBAppException("table not found");
		} else {
			throw new DBAppException("table does not have a clusterring key , fix the csv");

		}
	}

	// given a specific String keyName and a String script as well as a String
	// value
	// this method should return true if the value of the object is repeated
	// within
	// the script

	public boolean valueIsRepeated(String keyName, String script, String value) throws DBAppException {

		try {
			String[] entries = script.split("[#$]");

			// now entries[i] = key,value
			// however some rows are empty strings
			for (int i = 0; i < entries.length; i++) {

				if (entries[i].compareTo("") != 0) {
					String keyValue[] = entries[i].split(",");
					if (keyValue[0].compareTo(keyName) == 0 && keyValue[1].compareTo(value) == 0) {
						return true;
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;

	}

	public String findClusteringKeyName(String strTableName) throws DBAppException {
		int row = findMetaDataRow(strTableName);

		Table table = findTable(strTableName);

		for (int i = row; i < table.ColNum + row; i++) {
			List<String[]> temp = readCSV();
			String indexname = temp.get(i)[3].toString();

			if (indexname.compareTo("True") == 0) {
				// this is the clusterring key , return its name

				return temp.get(i)[1].toString();

			}
		}
		throw new DBAppException("Clusterring key not found (method not working properly or faulty input)");
	}

	// The Method addTable adds a table into array tableList in a way keeping it
	// sorted.
	public void addTable(Table table) {
		int size = tableList.length;

		if (size == 0) {
			tableList = new Table[1];
			tableList[0] = table;
			return;
		}

		Table[] temp = new Table[size + 1];
		String strTableName = table.getTableName();

		int i;

		for (i = size - 1; (i >= 0 && tableList[i].getTableName().compareTo(strTableName) >= 0); i--) {
			if (tableList[i].getTableName().compareTo(strTableName) == 0) {
				System.out.println("There is table already created with the same name");
			} else if (tableList[i].getTableName().compareTo(strTableName) > 0) {
				temp[i + 1] = tableList[i];
			}
		}

		temp[i + 1] = table;

		for (int j = i; j >= 0; j--) {
			temp[j] = tableList[j];
		}
		tableList = temp;

	}

	// The method findTable searched in tableList using binary search
	public Table findTable(String strTableName) throws DBAppException {

		return findTableHelper(0, this.tableList.length - 1, strTableName);

	}

	public Table findTableHelper(int low, int high, String strTableName) throws DBAppException {
		if (high < low) {
			throw new DBAppException("No table found with this name");
		}
		int mid = (low + high) / 2;

		if (tableList[mid].getTableName().compareTo(strTableName) == 0)
			return tableList[mid];
		if (tableList[mid].getTableName().compareTo(strTableName) < 0)
			return findTableHelper((mid + 1), high, strTableName);

		return findTableHelper(low, (mid - 1), strTableName);
	}

	public int findTablePosition(String strTableName) throws DBAppException {

		return findTablePositionHelper(0, tableList.length - 1, strTableName);

	}

	public int findTablePositionHelper(int low, int high, String strTableName) throws DBAppException {
		if (high < low) {
			throw new DBAppException("No table found with this name");
		}
		int mid = (low + high) / 2;

		if (tableList[mid].getTableName().compareTo(strTableName) == 0)
			return mid;
		if (tableList[mid].getTableName().compareTo(strTableName) < 0)
			return findTablePositionHelper((mid + 1), high, strTableName);

		return findTablePositionHelper(low, (mid - 1), strTableName);
	}

	public int findMetaDataRow(String strTableName) throws DBAppException {
		int position = findTablePosition(strTableName);
		int totalCol = 0;
		for (int i = 0; i < position; i++) {
			totalCol += this.tableList[i].getColNum();
		}
		return totalCol;
	}

	public void EditMetaData(String strTableName, String strColName, String strIndexName, int rowPosition)
			throws Exception {
		String filePath = metaData;

		try {
			// Read the existing CSV file
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			List<String> lines = new ArrayList<>();
			String line;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
			reader.close();

			// Edit the specified row
			int rowToEdit;
			for (rowToEdit = rowPosition; (lines.get(rowToEdit).split(","))[1]
					.compareTo(strColName) != 0; rowToEdit++) {
				System.out
						.println((lines.get(rowToEdit).split(","))[0] + "    " + (lines.get(rowToEdit).split(","))[1]);
				if ((lines.get(rowToEdit).split(","))[0].compareTo(strTableName) != 0) {
					throw new Exception("There is no row in this table with this name");
				}
			}
			// New values for the row
			String[] rowData = lines.get(rowToEdit).split(",");
			String[] newValues = { rowData[0], rowData[1], rowData[2], rowData[3], strIndexName, "B+tree" };

			if (rowToEdit >= 0 && rowToEdit < lines.size()) {
				lines.set(rowToEdit, String.join(",", newValues));
			} else {
				System.err.println("Row index out of bounds");
				return;
			}

			// Write the updated content back to the CSV file
			BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
			for (String updatedLine : lines) {
				writer.write(updatedLine);
				writer.newLine();
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String[]> readCSV() {
		List<String[]> data = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(this.metaData))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] row = line.split(",");
				data.add(row);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}

	// this method should be used to find the table instance from the
	// csv file using the table's name to validate entry types

	// MUST BE TESTED
	public boolean validateFromCsv(String tableName, Object value, String columnName)
			throws IOException, DBAppException {

		String directoryPath = metaData;
		BufferedReader br = new BufferedReader(new FileReader(directoryPath));
		String line = br.readLine();
		boolean flag = false;
		boolean columnFlag = false;
		while (line != null) {
			String[] colInfo = line.split(",");

			if (colInfo[0].equals(tableName)) {
				// found the table

				flag = true;
				if (colInfo[1].equals(columnName))
					columnFlag = true;

				if (colInfo[2].compareTo("java.lang.Integer") == 0) {
					if (value instanceof Integer && columnName.equals(colInfo[1])) {
						columnFlag = true;
						br.close();
						return true;
					}
				} else if (colInfo[2].compareTo("java.lang.String") == 0) {
					if (value instanceof String && columnName.equals(colInfo[1])) {
						columnFlag = true;

						br.close();
						return true;
					}
				} else if (colInfo[2].compareTo("java.lang.Double") == 0) {
					if (value instanceof Double && columnName.equals(colInfo[1])) {
						columnFlag = true;

						br.close();
						return true;
					}
				}
			}
			if (!(colInfo[0].equals(tableName)) && flag == true) {
				br.close();
				if (!columnFlag)
					throw new DBAppException("wrong column name");

				return false;
			}
			line = br.readLine();
		}

		if (flag == false)
			throw new DBAppException("wrong table name");
		if (!columnFlag)
			throw new DBAppException("wrong column name");

		br.close();
		return false;
	}

	public ArrayList<String> logicOperratorHelper(ArrayList<String> result1, ArrayList<String> result2, String operator)
			throws DBAppException {

		ArrayList<String> output = new ArrayList<String>();

		try {

			switch (operator) {
			case "AND":
				for (int i = 0; i < result1.size(); i++) {
					if (existsInArrayList(result1.get(i), result2)) {
						output.add(result1.get(i));
					}
				}
				break;
			case "OR":
				for (int i = 0; i < result1.size(); i++) {
					output.add(result1.get(i));
				}
				for (int i = 0; i < result2.size(); i++) {
					if (!existsInArrayList(result2.get(i), output)) {
						output.add(result2.get(i));
					}
				}
				break;
			case "XOR":
				for (int i = 0; i < result1.size(); i++) {
					if (!existsInArrayList(result1.get(i), result2)) {
						output.add(result1.get(i));
					}
				}
				for (int i = 0; i < result2.size(); i++) {
					if (!existsInArrayList(result2.get(i), result1)) {
						output.add(result2.get(i));
					}
				}

				break;

			default:
				throw new DBAppException("Operator not supported");
			}
		} catch (Exception e) {
			throw new DBAppException("");
		}
		return output;
	}

	public boolean existsInArrayList(String s, ArrayList<String> array) {
		for (int i = 0; i < array.size(); i++) {
			if (array.get(i).compareTo(s) == 0) {
				return true;
			}
		}
		return false;

	}

	// The Method addTable adds a table into array tableList in a way keeping it
	// sorted.
	public void addIndexPair(IndexPair indexPair) {
		int size = indexPairs.length;

		if (size == 0) {
			indexPairs = new IndexPair[1];
			indexPairs[0] = indexPair;
			return;
		}

		IndexPair[] temp = new IndexPair[size + 1];
		String strIndex = indexPair.indexName;

		int i;

		for (i = size - 1; (i >= 0 && indexPairs[i].indexName.compareTo(strIndex) >= 0); i--) {
			if (indexPairs[i].indexName.compareTo(strIndex) == 0) {
				System.out.println("There is table already created with the same name");
			} else if (indexPairs[i].indexName.compareTo(strIndex) > 0) {
				temp[i + 1] = indexPairs[i];
			}
		}

		temp[i + 1] = indexPair;

		for (int j = i; j >= 0; j--) {
			temp[j] = indexPairs[j];
		}
		indexPairs = temp;

	}

	// The method findTable searched in tableList using binary search
	// low=0
	// high= indexPairs.length
	public IndexPair findIndexPair(int low, int high, String strIndex) throws DBAppException {
		if (high < low) {
			throw new DBAppException("No table found with this name");
		}
		int mid = (low + high) / 2;

		if (indexPairs[mid].indexName.compareTo(strIndex) == 0)
			return indexPairs[mid];
		if (indexPairs[mid].indexName.compareTo(strIndex) < 0)
			return findIndexPair((mid + 1), high, strIndex);

		return findIndexPair(low, (mid - 1), strIndex);
	}

	public void insertIntoIndex() {
		// needs to be implemented
	}

	public String findColIndex(String strTableName, String colName) throws IOException {
		String directoryPath = metaData;
		BufferedReader br = new BufferedReader(new FileReader(directoryPath));
		String line = br.readLine();
		boolean flag = false;
		while (line != null) {
			String[] colInfo = line.split(",");
			if (colInfo[0].equals(strTableName) && colInfo[1].equals(colName)) {
				return colInfo[4];
			}
			line = br.readLine();
		}
		br.close();
		return "null";
	}

	// TODO KEEP STATE
	// TODO MUST BE CHECKED AND TESTED IN USE
	public void serialize() {
		try {

			String directoryPath = "D:\\Semester 6\\DB\\";
			String dbAppPath = directoryPath + "DBApp.class";
			FileOutputStream fileOut = new FileOutputStream(dbAppPath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public static DBApp deserializeAll() {
		DBApp app = null;
		try {
			FileInputStream fileIn = new FileInputStream(appPath);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			try {
				app = (DBApp) in.readObject();
			} catch (ClassNotFoundException e) {

				e.printStackTrace();
			}
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();

		}
		return app;
	}

	public boolean isIndexed(String tableName, String column) throws IOException {
		if (findColIndex(tableName, column).equals("null")) {
			return false;
		}
		return true;
	}

	public static double compare(Object x , Object y) {
		if(x instanceof String) {
			return ((String) x).compareTo((String)y);
		}else if(x instanceof Double){
			return (Double)x-(Double)y;
		}else {
			return (double)((Integer)x-(Integer)y);
		}
	}

	public static void main(String[] args) {

		try {
			DBApp dbApp = new DBApp();
	      
	        
			String strTableName = "Student";

			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable(strTableName, "id", htblColNameType);
			
			
			dbApp.createIndex(strTableName, "gpa", "gpaIndex");

			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("gpa", new Double(0.95));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("id", new Integer(2343432));
			

			Hashtable htblColNameValue1 = new Hashtable();
			htblColNameValue1.put("name", new String("Mohamed"));
			htblColNameValue1.put("id", new Integer(34635386));
			htblColNameValue1.put("gpa", new Double(2.1));

			Hashtable htblColNameValue2 = new Hashtable();
			htblColNameValue2.put("name", new String("Mona"));
			htblColNameValue2.put("id", new Integer(350000));
			htblColNameValue2.put("gpa", new Double(4.0));

			Hashtable htblColNameValue3 = new Hashtable();
			htblColNameValue3.put("name", new String("Marly"));
			htblColNameValue3.put("id", new Integer(3405673));
			htblColNameValue3.put("gpa", new Double(1.5));
			
			
			dbApp.insertIntoTable(strTableName, htblColNameValue1);
			dbApp.insertIntoTable(strTableName, htblColNameValue2);
		    dbApp.insertIntoTable(strTableName, htblColNameValue);
		    dbApp.insertIntoTable(strTableName, htblColNameValue3);
			
			Hashtable delete = new Hashtable();
			delete.put("id", 34635386);
			
			dbApp.deleteFromTable(strTableName, delete);
//			
			
			
			//System.out.println("table: "+dbApp.tableList[0].toString());
			
			dbApp.serialize();
			
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		
		
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0]._strTableName = "Student";
//		arrSQLTerms[0]._strColumnName = "name";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "Mona";
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName = "gpa";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new Double(1.5);
//		String[] strarrOperators = new String[1];
//		strarrOperators[0] = "OR";
//		// select * from Student where name = “John Noor” or gpa = 1.5;
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//		while (resultSet.hasNext()) {
//			String element = (String) resultSet.next();
//			System.out.println(element);
//		}
		
	}

}