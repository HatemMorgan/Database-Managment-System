package Database2Project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectInputStream.GetField;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.Buffer;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.net.URL;

import javax.print.attribute.standard.MediaSize.Other;
import javax.xml.crypto.dsig.keyinfo.PGPData;

import BTree.BTree;
import Exceptions.DBEngineException;

public class Table implements Serializable {
	Hashtable<String, String> fTblColNameType;
	Hashtable<String, String> fTblColNameRefs;
	Hashtable<String, String> fTblColIndexPath;
	String TableName;
	String Key;
	ArrayList<String> TablePagesPath;
	int pageNumber;
	String WorkSpacePath = System.getProperty("user.dir");
	static int count = 0;
	int MaximumRowsCountinPage;
	int numberOfRecordsInTable;

	public Table(Hashtable<String, String> fTblColNameType, Hashtable<String, String> fTblColNameRefs, String TableName,
			String Key) throws IOException {

		String row = "";
		File f1 = new File("config/DBApp.config");
		FileReader fr = new FileReader(f1);
		Properties p1 = new Properties();
		p1.load(fr);
		MaximumRowsCountinPage = Integer.parseInt(p1.getProperty("MaximumRowsCountinPage"));
		fr.close();

		this.fTblColNameType = fTblColNameType;
		this.fTblColNameRefs = fTblColNameRefs;
		this.fTblColIndexPath = new Hashtable<String, String>();
		this.TableName = TableName;
		this.Key = Key;
		TablePagesPath = new ArrayList<String>();
		pageNumber = 0;
		numberOfRecordsInTable = 0;

	}

	public int CreateNewPage() throws FileNotFoundException, IOException {
		pageNumber++;
		Page page = new Page(TableName);
		Hashtable<String, Object> pageHeaders = new Hashtable<String, Object>();

		Set<String> keys = fTblColNameType.keySet();
		Iterator<String> iterator = keys.iterator();

		while (iterator.hasNext()) {

			String columnName = iterator.next();
			pageHeaders.put(columnName, "");

		}
		pageHeaders.put("Touch_Date", "");

		page.Rows.add(pageHeaders);

		String FolderPath = WorkSpacePath + "/classes/Hatem 31-4562 Omar 31-5039/" + this.TableName + "/Page"
				+ pageNumber + ".class";

		serializePage(FolderPath, page);

		TablePagesPath.add(FolderPath);
		return pageNumber;
	}

	public void insertIntoPage(Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, ClassNotFoundException, IOException {
        numberOfRecordsInTable++;
        
		Page targetPage = null;
		Boolean checkForPageSize = false;

		// usedPagePath stores the path of the page that the row was inserted in
		// it
		String usedPagePath = "";
		Page UsedPage = null;

		String pagePath = TablePagesPath.get(TablePagesPath.size() - 1);
		targetPage = GetTargetPage(pagePath);
		if (targetPage.numberOfAvailableRows < MaximumRowsCountinPage) {

			targetPage.numberOfAvailableRows++;
			checkForPageSize = true; // check that the page contains number of
										// rows less than or equal 200
			usedPagePath = pagePath; // save the path of the page that we will
										// insert the new row in it
			UsedPage = targetPage; // save the page that we will insert into it
									// the new row

			if (fTblColNameType.size() != htblColNameValue.size()) {
				System.out.println("Insertion to table " + TableName + " failed due to incomplete inserted values");
				return;
			}
		}

		if (!checkForPageSize) {

			int PageIndex = CreateNewPage();
			String PagePath = TablePagesPath.get(PageIndex - 1);

			usedPagePath = PagePath; // save the path of the page that we will
										// insert the new row in it

			targetPage = GetTargetPage(PagePath);

			/*
			 * incremet page numberOfAvailableRows because we add headers to the
			 * page when we created
			 */

			targetPage.numberOfAvailableRows++;
			UsedPage = targetPage; // save the page that we will insert into it
									// the new row
			System.out.println("new Page Created");
		}

		DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		Calendar calobj = Calendar.getInstance();
		htblColNameValue.put("Touch_Date", df.format(calobj.getTime()));

		boolean CheckIfExistFalse = true;
		htblColNameValue.put("Exists", CheckIfExistFalse);

		// insert into page
		targetPage.Rows.add(htblColNameValue);

		/*
		 * insert into Btree
		 */

		Set<String> columnNamesForfTblColIndex = fTblColIndexPath.keySet();
		Iterator<String> fTblColIndexIterator = columnNamesForfTblColIndex.iterator();

		while (fTblColIndexIterator.hasNext()) {

			String indexedColumnName = fTblColIndexIterator.next();
			Object IndexedColumnKey = htblColNameValue.get(indexedColumnName);

			String TargetIndexedColumnBTreePath = fTblColIndexPath.get(indexedColumnName);
			BTree TargetIndexedColumnBTree = GetIndexByDeSerialization(TargetIndexedColumnBTreePath);

			int tupleNumber = targetPage.numberOfAvailableRows + 200 * (TablePagesPath.size() - 1);
			TargetIndexedColumnBTree.insert(Integer.parseInt(IndexedColumnKey + ""), tupleNumber);
			// System.out.println("here
			// "+Integer.parseInt(IndexedColumnKey+"")+" "+tupleNumber);
			// Serialization of Btree of the Target indexed column

			String filePath = WorkSpacePath + "/classes/Hatem 31-4562 Omar 31-5039/" + this.TableName + "/"
					+ indexedColumnName + "_index.class";
			serializeBTree(filePath, TargetIndexedColumnBTree);

		}

		serializePage(usedPagePath, UsedPage);

		System.out.println("One row inserted successfully to " + TableName + " table " + count++);
	}

	public Iterator SelectFromPages(Hashtable<String, Object> htblColNameValue, String strOperator)
			throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException {

		ArrayList<Hashtable<String, Object>> returnedRows = getSatisfiedRowsIndex(htblColNameValue, strOperator);
		Page newPage = new Page("");
		newPage.setRowsArrayList(returnedRows);
		ArrayList<String> printedRows = newPage.getRows();
		return printedRows.iterator();

	}

	public ArrayList<Hashtable<String, Object>> getSatisfiedRowsIndex(Hashtable<String, Object> htblColNameValue,
			String strOperator) throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException {
		ArrayList<Hashtable<String, Object>> returnedSatisfiedRows = new ArrayList<Hashtable<String, Object>>();

		Set<String> keys = htblColNameValue.keySet();
		Iterator<String> condiondColumnsiterator = keys.iterator();

		if (strOperator.isEmpty()) {
			String columnName = condiondColumnsiterator.next();
			Object columnValue = htblColNameValue.get(columnName);

			if (fTblColIndexPath.containsKey(columnName)) {

				Hashtable<String, Object> returnedrowFromIndexedSearch = selectRowsUsingIndexedColumn(columnName,
						columnValue);

				returnedSatisfiedRows.add(returnedrowFromIndexedSearch);

			} else {
				returnedSatisfiedRows = SelectRowsUsingNonIndexedColumn(htblColNameValue, strOperator);

			}

			return returnedSatisfiedRows;
		}

		/*
		 * I assume that the indexed Column will be the first column in the
		 * where closure
		 */

		if (strOperator.equals("AND")) {

			String firstConditionColumnName = condiondColumnsiterator.next();
			Object firstConditionColumnValue = htblColNameValue.get(firstConditionColumnName);

			/*
			 * check if the first column is index
			 */

			if (fTblColIndexPath.containsKey(firstConditionColumnName)) {
				Hashtable<String, Object> returnedrowFromIndexedSearch = null;
				try {
					returnedrowFromIndexedSearch = selectRowsUsingIndexedColumn(firstConditionColumnName,
							firstConditionColumnValue);
				} catch (DBEngineException e) {

				}

				/*
				 * check if the other conditions satisfies this row as the
				 * indexed column is unique no duplicates
				 */
				if (returnedrowFromIndexedSearch != null) {
					boolean rowIsValied = true;
					while (condiondColumnsiterator.hasNext()) {
						String conditionColumnName = condiondColumnsiterator.next();
						Object conditionColumnValue = htblColNameValue.get(conditionColumnName);

						Object returnedRowColumnValue = returnedrowFromIndexedSearch.get(conditionColumnName);

						if (!returnedRowColumnValue.equals(conditionColumnValue)) {
							System.out.println("No rows satisfied the where condition");
							rowIsValied = false;
							break;
						}

					}
					/*
					 * return the row after it satisfies all and conditons
					 */
					if (rowIsValied == true) {
						returnedSatisfiedRows.add(returnedrowFromIndexedSearch);

					} else {

						/*
						 * intialize an empty arraylist in order to not return
						 * null because it is not handled in the DBApp test
						 * cases so i will return an empty iterator
						 */
						returnedSatisfiedRows = new ArrayList<Hashtable<String, Object>>();
					}

				} else {
					/*
					 * intialize an empty arraylist in order to not return null
					 * because it is not handled in the DBApp test cases so i
					 * will return an empty iterator
					 */
					returnedSatisfiedRows = new ArrayList<Hashtable<String, Object>>();
				}

			} else {
				/*
				 * there is no indexed columns in the where closure
				 */

				returnedSatisfiedRows = SelectRowsUsingNonIndexedColumn(htblColNameValue, strOperator);
				if (returnedSatisfiedRows.size() == 0) {
					System.out.println("No records satisfied the conditions");
				}

			}

			return returnedSatisfiedRows;

		}
		/*
		 * if the strOperator is OR
		 */

		if (strOperator.equals("OR")) {
			ArrayList<Hashtable<String, Object>> returnedRows = new ArrayList<Hashtable<String, Object>>();
			Hashtable<String, Object> notIndexedhtblColumnNameValue = new Hashtable<String, Object>();
			/*
			 * check for indexed columns
			 */

			while (condiondColumnsiterator.hasNext()) {
				// System.out.println("here"+condiondColumnsiterator.next());
				String conditionColumnName = condiondColumnsiterator.next();

				Object conditionColumnValue = htblColNameValue.get(conditionColumnName);

				if (fTblColIndexPath.containsKey(conditionColumnName)) {

					try {
						Hashtable<String, Object> returnedrowFromIndexedSearch = selectRowsUsingIndexedColumn(
								conditionColumnName, conditionColumnValue);
						returnedRows.add(returnedrowFromIndexedSearch);
					} catch (DBEngineException ex) {

					}

				} else {
					notIndexedhtblColumnNameValue.put(conditionColumnName, conditionColumnValue);
				}
			}

			/*
			 * there is no indexed columns in the where closure
			 */

			ArrayList<Hashtable<String, Object>> returnedRowsFromNonIndexedSearch = SelectRowsUsingNonIndexedColumn(
					notIndexedhtblColumnNameValue, strOperator);
			returnedRows.addAll(returnedRowsFromNonIndexedSearch);

			/*
			 * check if there are row duplicated and remove them
			 */

			ArrayList<Hashtable<String, Object>> satisifedCondionsRows = new ArrayList<Hashtable<String, Object>>();

			for (int i = 0; i < returnedRows.size(); i++) {

				Hashtable<String, Object> row = returnedRows.get(i);
				Object rowIdValue = row.get("ID");
				boolean checkFlag = false;

				for (int j = i + 1; j < returnedRows.size(); j++) {
					Hashtable<String, Object> row2 = returnedRows.get(j);
					Object rowIdValue2 = row2.get("ID");
					// System.out.println(rowIdValue +" "+rowIdValue2);
					if (rowIdValue.equals(rowIdValue2)) {
						checkFlag = true;
						break;
					}

				}

				/*
				 * check on flag if it is true means that the row is replicated
				 * if it is false means that the row is not replicated so add it
				 * to satisifedCondionsRows arraylist
				 */
				if (checkFlag == false) {
					satisifedCondionsRows.add(row);
				}
			}

			return satisifedCondionsRows;
		}
		return null;

	}

	public Hashtable<String, Object> selectRowsUsingIndexedColumn(String ColumnName, Object columnValue)
			throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException {

		String BtreePath = fTblColIndexPath.get(ColumnName);
		BTree targetIndexedBtree = GetIndexByDeSerialization(BtreePath);
		int tupleNumber = 0;
		
		try {

			 if(columnValue instanceof String){
				 tupleNumber = (Integer) targetIndexedBtree.search((Comparable) columnValue);
             }else{
             	if(columnValue instanceof Integer){
             		tupleNumber = (Integer) targetIndexedBtree.search(Integer.parseInt(columnValue + ""));
             	}else{
             		if(columnValue instanceof Double){
             			System.out.println("hereee");
             			tupleNumber = (Integer) targetIndexedBtree.search(Double.parseDouble(columnValue + ""));
             		}else{
             			 tupleNumber = (Integer) targetIndexedBtree.search((Comparable) columnValue);
             		}
             	}
             }
			
			
		} catch (Exception e) {

			throw new DBEngineException("No rows Satisfies the conditons");
		}

		int pageNumber = tupleNumber / MaximumRowsCountinPage;
		if (tupleNumber % MaximumRowsCountinPage != 0) {
			pageNumber++;
		}
		// System.out.println("page number = "+pageNumber);
		// System.out.println("tuple number = "+tupleNumber);
		// System.out.println("row number = "+
		// tupleNumber%MaximumRowsCountinPage);

		/*
		 * get the target page that contains the returned tuple
		 */
		String targetPagePath = TablePagesPath.get(pageNumber - 1);
		Page targetPage = GetTargetPage(targetPagePath);

		// System.out.println("page =
		// "+targetPage.getRowsArrayList().toString());

		/*
		 * get the target row
		 */

		ArrayList<Hashtable<String, Object>> targetPageRows = targetPage.getRowsArrayList();
		Hashtable<String, Object> targetrow = targetPageRows.get(tupleNumber % MaximumRowsCountinPage);
		// System.out.println("row = "+targetrow.toString());
		return targetrow;

	}
	/*
	 * this method return a list of all rows that satisfy the conditon on an non
	 * indexed columns with AND and OR operators
	 * 
	 */

	public ArrayList<Hashtable<String, Object>> SelectRowsUsingNonIndexedColumn(
			Hashtable<String, Object> htblColNameValue, String strOperator)
			throws FileNotFoundException, ClassNotFoundException, IOException {
		ArrayList<Hashtable<String, Object>> returnedRowsList = new ArrayList<Hashtable<String, Object>>();
		for (int i = 0; i < TablePagesPath.size(); i++) {
			String PagePath = TablePagesPath.get(i);
			Page targetPage = GetTargetPage(PagePath);

			ArrayList<Hashtable<String, Object>> returnedRowsFromApage = selectRowsfromPageByCondition(targetPage,
					htblColNameValue, strOperator);

			returnedRowsList.addAll(returnedRowsFromApage);

		}

		return returnedRowsList;
	}

	/**
	 * 
	 * select row from a page by condition
	 * 
	 * @param targetPage
	 * @param htblColNameValue
	 * @param strOperator
	 * @return
	 */

	public ArrayList<Hashtable<String, Object>> selectRowsfromPageByCondition(Page targetPage,
			Hashtable<String, Object> htblColNameValue, String strOperator) {
		ArrayList<Hashtable<String, Object>> PageRows = targetPage.Rows;
		Page resultPage = new Page("result");
		ArrayList<Hashtable<String, Object>> resultedRowsList = new ArrayList<Hashtable<String, Object>>();

		// it is used to check that the row satisfy the condition or not by
		// concatenating True and false

		for (int i = 1; i < PageRows.size(); i++) {
			boolean checkForValidRowWhenUsingAND = true;
			boolean checkForValidRowWhenUsingOR = false;
			Hashtable<String, Object> Row = PageRows.get(i);
			Set<String> ColumnNames = htblColNameValue.keySet();
			Iterator<String> htblColNameValueIterator = ColumnNames.iterator();

			while (htblColNameValueIterator.hasNext()) {
				String ColumnName = htblColNameValueIterator.next();

				// to check for invalid columns that are not in the table we are
				// selecting from
				if (!Row.containsKey(ColumnName)) {
					System.out.println("Invalid Column Name");
				}

				// skip this row if its Exists is false which means that it was
				// deleted

				Object ConditionValue = htblColNameValue.get(ColumnName);
				Object ColumnValueForCurrentRow = Row.get(ColumnName);

				// to check that there is only one column in the conditions

				if (htblColNameValue.size() == 1 && strOperator.isEmpty()
						&& ConditionValue.equals(ColumnValueForCurrentRow)) {
					if (!Row.get("Exists").equals(false)) {
						resultedRowsList.add(Row);
					}

				}

				if ((!ConditionValue.equals(ColumnValueForCurrentRow)) && strOperator.equals("AND")) {
					checkForValidRowWhenUsingAND = false;

					break;
				} else {
					if ((ConditionValue.equals(ColumnValueForCurrentRow)) && strOperator.equals("OR")) {
						checkForValidRowWhenUsingOR = true;

						break;
					}
				}
				// System.out.println(ConditionValue +"
				// "+ColumnValueForCurrentRow + checkForValidRowWhenUsingAND);
			}
			if (Row.get("Exists").equals(false)) {
				continue;
			}

			if (checkForValidRowWhenUsingAND == true && strOperator.equals("AND")) {

				resultedRowsList.add(Row);
			}

			if (checkForValidRowWhenUsingOR == true && strOperator.equals("OR")) {
				resultedRowsList.add(Row);
			}

		}
		// get the rows in the resultPage in the form of arraylist of strings

		return resultedRowsList;
	}

	public void UpdatePagesWithTheTargetRow(Object strKey, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException {

		Hashtable<String, Object> targetRow = selectRowsUsingIndexedColumn("ID", strKey);

		if (targetRow.get("Exists").equals(true)) {
			Set<String> keys = htblColNameValue.keySet();
			Iterator<String> ColumnIterator = keys.iterator();

			while (ColumnIterator.hasNext()) {
				String ColumnName = ColumnIterator.next();
				Object newColumnValue = htblColNameValue.get(ColumnName);

				/*
				 * if he will update the index column so we will update the its
				 * BTree
				 */

				if (fTblColIndexPath.containsKey(ColumnName)) {
					
					Object indexedColumnOldValue = targetRow.get(ColumnName);
					
					String BtreePath = fTblColIndexPath.get(ColumnName);
					BTree targetIndexedBtree = GetIndexByDeSerialization(BtreePath);

					// Delete the Old key
		
					  if(indexedColumnOldValue instanceof String){
							targetIndexedBtree.delete((Comparable) indexedColumnOldValue );
	                    }else{
	                    	if(indexedColumnOldValue instanceof Integer){
	                    		
	                    		targetIndexedBtree.delete(Integer.parseInt(indexedColumnOldValue + ""));
	                    	}else{
	                    		if(indexedColumnOldValue instanceof Double){
	                    			targetIndexedBtree.delete(Double.parseDouble(indexedColumnOldValue + ""));
	                    		}else{
	                    			targetIndexedBtree.delete((Comparable) indexedColumnOldValue );
	                    		}
	                    	}
	                    }

					// Insert the new key
					  
						int ID = Integer.parseInt(strKey+"");
						
						/*
						 * increment rowID by 1 because the first row in each page is the headers 
						 * then add it to 200*(the page index number)
						 */
							int tupleNumber = ID+1 ;

					 if(newColumnValue instanceof String){
						 targetIndexedBtree.insert((Comparable) newColumnValue, tupleNumber);
	                    }else{
	                    	if(newColumnValue instanceof Integer){
	                    		targetIndexedBtree.insert(Integer.parseInt(newColumnValue + ""), tupleNumber);
	                    	}else{
	                    		if(newColumnValue instanceof Double){
	                    			targetIndexedBtree.insert(Double.parseDouble(newColumnValue + ""), tupleNumber);
	                    		}else{
	                    			 targetIndexedBtree.insert((Comparable) newColumnValue, tupleNumber);
	                    		}
	                    	}
	                    }
					 
					

					serializeBTree(BtreePath, targetIndexedBtree);
				}

				targetRow.replace(ColumnName, newColumnValue);

			}
			DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
			Calendar calobj = Calendar.getInstance();
			targetRow.replace("Touch_Date", df.format(calobj.getTime()));

			ArrayList<Hashtable<String, Object>> listOfUpgradedRows = new ArrayList<Hashtable<String, Object>>();
			listOfUpgradedRows.add(targetRow);

			upgradeThePage(listOfUpgradedRows);

			System.out.println("One Row Affected");
		} else {
			System.out.println("Zero Rows Affected");

		}

	}

	public void DeleteTargetRowFromPage(Hashtable<String, Object> htblColNameValue, String strOperator)
			throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException {

		ArrayList<Hashtable<String, Object>> returnedRows = getSatisfiedRowsIndex(htblColNameValue, strOperator);

		Iterator<Hashtable<String, Object>> iterator = returnedRows.iterator();

		ArrayList<Hashtable<String, Object>> upgradedRows = new ArrayList<Hashtable<String, Object>>();

		int countDeletedRows = 0;

		while (iterator.hasNext()) {
			Hashtable<String, Object> row = iterator.next();

			// check if the row is not deleted before
			if (row.get("Exists").equals(true)) {
				/*
				 * Delete from page
				 */
				row.replace("Exists", false);
				upgradedRows.add(row);
				countDeletedRows++;

				/*
				 * Delete from BTree
				 */
				Set<String> indexedColumnkeys = fTblColIndexPath.keySet();
				Iterator<String> indexedColumnsIterator = indexedColumnkeys.iterator();
				while (indexedColumnsIterator.hasNext()) {
					String indexedColumnName = indexedColumnsIterator.next();
					String BtreePath = fTblColIndexPath.get(indexedColumnName);
					BTree targetIndexedBtree = GetIndexByDeSerialization(BtreePath);

					Object indexColumnValue = row.get(indexedColumnName);
					
					   if(indexColumnValue instanceof String){
							targetIndexedBtree.delete((Comparable) indexColumnValue );
	                    }else{
	                    	if(indexColumnValue instanceof Integer){
	                    		targetIndexedBtree.delete(Integer.parseInt(indexColumnValue + ""));
	                    	}else{
	                    		if(indexColumnValue instanceof Double){
	                    			targetIndexedBtree.delete(Double.parseDouble(indexColumnValue + ""));
	                    		}else{
	                    			targetIndexedBtree.delete((Comparable) indexColumnValue );
	                    		}
	                    	}
	                    }
					
					serializeBTree(BtreePath, targetIndexedBtree);
				}

			}
		}

		upgradeThePage(upgradedRows);

		/*
		 * boolean deletedRows =false; for(int i = 0 ; i<TablePagesPath.size();
		 * i++){ String path = TablePagesPath.get(i); Page TargetPage =
		 * GetTargetPage(path);
		 * 
		 * ArrayList<Integer> RowsIndexArray =
		 * CheckAvailableConditionRowInPage(TargetPage, htblColNameValue ,
		 * strOperator);
		 * 
		 * // check that the row exsist in TargetPage if(RowsIndexArray.size()
		 * !=0){ for(int j =0 ; j<RowsIndexArray.size() ; j++){
		 * Hashtable<String, Object> TargetRow =
		 * TargetPage.Rows.get((int)RowsIndexArray.get(j));
		 * TargetRow.replace("Exists", false);
		 * 
		 * System.out.println("One Row Affected"); deletedRows = true;
		 * serializePage(path, TargetPage);
		 * 
		 * } }
		 * 
		 * } if(deletedRows == false ){ System.out.println("Zero Rows Affected"
		 * ); }
		 */
	}

	// CheckAvailableConditionRowInPage method will check that the target row
	// exsist in the page or not
	// if it doesnot exsist it will return -1 otherwise it will return the index
	// of the rows in the page

	public ArrayList<Integer> CheckAvailableConditionRowInPage(Page TargetPage,
			Hashtable<String, Object> htblCondtionColNameValue, String strOperator) {

		ArrayList<Hashtable<String, Object>> TargetPageRows = TargetPage.Rows;

		ArrayList<Integer> RowsIndexArray = new ArrayList<Integer>();

		for (int i = 1; i < TargetPageRows.size(); i++) {

			Set<String> ColumnsName = htblCondtionColNameValue.keySet();
			Iterator<String> ColumnsIterator = ColumnsName.iterator();

			boolean checkForValidRowWhenUsingAND = true;
			boolean checkForValidRowWhenUsingOR = false;

			while (ColumnsIterator.hasNext()) {
				String CondtioncolumnName = ColumnsIterator.next();

				Object ConditionColumnValue = htblCondtionColNameValue.get(CondtioncolumnName);

				if (!TargetPageRows.get(i).containsKey(CondtioncolumnName)) {
					System.out.println("invalid Checking Columns");
					return RowsIndexArray;
				}

				Object ColumnValueForCurrentRow = TargetPageRows.get(i).get(CondtioncolumnName);

				if (htblCondtionColNameValue.size() == 1 && strOperator.isEmpty()
						&& ConditionColumnValue.equals(ColumnValueForCurrentRow)
						&& TargetPageRows.get(i).get("Exists").equals(true)) {
					RowsIndexArray.add((Integer) i);
					return RowsIndexArray;
				}

				if ((!ConditionColumnValue.equals(ColumnValueForCurrentRow)) && strOperator.equals("AND")
						&& TargetPageRows.get(i).get("Exists").equals(true)) {

					checkForValidRowWhenUsingAND = false;

					break;
				} else {
					if ((ConditionColumnValue.equals(ColumnValueForCurrentRow)) && strOperator.equals("OR")
							&& TargetPageRows.get(i).get("Exists").equals(true)) {
						checkForValidRowWhenUsingOR = true;

						break;
					}
				}

			}
			if (checkForValidRowWhenUsingAND == true && strOperator.equals("AND")) {

				RowsIndexArray.add((Integer) i);
			}

			if (checkForValidRowWhenUsingOR == true && strOperator.equals("OR")) {
				RowsIndexArray.add((Integer) i);
			}

		}
		return RowsIndexArray;
	}

	public void createIndex(String strColName) throws IOException, ClassNotFoundException {
		BTree columnBTreeIndex = new BTree();
        int count = 0;
		String filePath = WorkSpacePath + "/classes/Hatem 31-4562 Omar 31-5039/" + this.TableName + "/" + strColName
				+ "_index.class";
		fTblColIndexPath.put(strColName, filePath);
		
        /*
         * check if the the table contains records or still empty
         * if it contains records so we have to scan the whole table
         * to insert keys in the btree
         */
		if(numberOfRecordsInTable != 0){
			for(int i =0 ;i<TablePagesPath.size();i++){
				String pagePath = TablePagesPath.get(i);
				Page page = GetTargetPage(pagePath);
				Iterator<Hashtable<String, Object>> rowsIterator = page.getRowsArrayList().iterator();
				
				/*
				 *  skip the first row because it contains the headers only
				 */
				
				rowsIterator.next();
				
				while(rowsIterator.hasNext()){
					Hashtable<String,Object> row = rowsIterator.next();
				
					Object indexedColumnValue  = row.get(strColName);
					
					Object rowIDValue = row.get("ID");
			
					int ID = Integer.parseInt(rowIDValue+"");
					
				/*
				 * increment rowID by 1 because the first row in each page is the headers 
				 * then add it to 200*(the page index number)
				 */
					int tupleNumber = ID+1 ;
					
					/*
					 * check for key data type
					 */
				
                    if(indexedColumnValue instanceof String){
                    	columnBTreeIndex.insert((Comparable) indexedColumnValue,tupleNumber);
                    }else{
                    	if(indexedColumnValue instanceof Integer){
                    		columnBTreeIndex.insert(Integer.parseInt(indexedColumnValue+""),tupleNumber);
                    	}else{
                    		if(indexedColumnValue instanceof Double){
                    			columnBTreeIndex.insert(Double.parseDouble(indexedColumnValue+""),tupleNumber);
                    		}else{
                    			columnBTreeIndex.insert((Comparable) indexedColumnValue,tupleNumber);
                    		}
                    	}
                    }
					
					
			
				}
				
			}
		}
		
		serializeBTree(filePath, columnBTreeIndex);
	}

	public void upgradeThePage(ArrayList<Hashtable<String, Object>> upgradedRows)
			throws FileNotFoundException, ClassNotFoundException, IOException {
		Iterator<Hashtable<String, Object>> iterator = upgradedRows.iterator();
		while (iterator.hasNext()) {
			Hashtable<String, Object> row = iterator.next();
			Object rowIDValue = row.get("ID");

			int tupleNumber = (int) rowIDValue;

			int pageNumber = tupleNumber / MaximumRowsCountinPage;
			if (tupleNumber % MaximumRowsCountinPage != 0) {
				pageNumber++;
			}

			/*
			 * get the target page that contains the returned tuple
			 */

			String targetPagePath = TablePagesPath.get(pageNumber - 1);
			Page targetPage = GetTargetPage(targetPagePath);

			targetPage.upgradePage(row, tupleNumber % MaximumRowsCountinPage);
			serializePage(targetPagePath, targetPage);

		}
	}

	public BTree GetIndexByDeSerialization(String PagePath)
			throws FileNotFoundException, IOException, ClassNotFoundException {

		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(PagePath)));
		BTree btree = (BTree) ois.readObject();
		ois.close();
		return btree;
	}

	public void serializeBTree(String filePath, BTree columnBTreeIndex) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(filePath)));
		oos.writeObject(columnBTreeIndex);
		oos.flush();
		oos.close();
	}

	public void serializePage(String filePath, Page page) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(filePath)));
		oos.writeObject(page);
		oos.flush();
		oos.close();
	}

	public Page GetTargetPage(String PagePath) throws FileNotFoundException, IOException, ClassNotFoundException {

		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(PagePath)));
		Page TargetPage = (Page) ois.readObject();
		ois.close();
		return TargetPage;
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {

		/*
		 * 
		 * ArrayList<Hashtable<String, Object>> returnedRows = new
		 * ArrayList<Hashtable<String,Object>>(); Hashtable<String,Object> row1
		 * = new Hashtable<String,Object>(); row1.put("ID",Integer.valueOf( "1"
		 * )); returnedRows.add(row1);
		 * 
		 * Hashtable<String,Object> rows2 = new Hashtable<String,Object>();
		 * rows2.put("ID",Integer.valueOf( "2" )); returnedRows.add(rows2);
		 * 
		 * Hashtable<String,Object> row3 = new Hashtable<String,Object>();
		 * row3.put("ID",Integer.valueOf( "2" )); returnedRows.add(row3);
		 * 
		 * Hashtable<String,Object> row4 = new Hashtable<String,Object>();
		 * row4.put("ID",Integer.valueOf( "1" )); returnedRows.add(row4);
		 * 
		 * Hashtable<String,Object> row5 = new Hashtable<String,Object>();
		 * row5.put("ID",Integer.valueOf( "1" )); returnedRows.add(row5);
		 * 
		 * ArrayList<Hashtable<String, Object>> satisifedCondionsRows = new
		 * ArrayList<Hashtable<String,Object>>();
		 * 
		 * for(int i=0 ; i<returnedRows.size();i++){ Hashtable<String, Object>
		 * row = returnedRows.get(i); Object rowIdValue = row.get("ID"); boolean
		 * checkFlag = false;
		 * 
		 * for(int j=i+1 ; j<returnedRows.size() ; j++){ Hashtable<String,
		 * Object> row2 = returnedRows.get(j); Object rowIdValue2 =
		 * row2.get("ID"); //System.out.println(rowIdValue +"  "+rowIdValue2);
		 * if(rowIdValue.equals(rowIdValue2)){ checkFlag = true; break; } } /*
		 * check on flag if it is true means that the row is replicated if it is
		 * false means that the row is not replicated so add it to
		 * satisifedCondionsRows arraylist
		 * 
		 * if(checkFlag==false){ satisifedCondionsRows.add(row); } }
		 * 
		 * Iterator<Hashtable<String, Object>> iterator =
		 * satisifedCondionsRows.iterator(); while(iterator.hasNext()){
		 * System.out.println(iterator.next().get("ID")); }
		 */

		
		 
	}

}
