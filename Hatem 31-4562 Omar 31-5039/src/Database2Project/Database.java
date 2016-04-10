package Database2Project;

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
import java.sql.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import BTree.BTree;
import Exceptions.*;

import java.net.URL;



public class Database implements Serializable {
	
	Hashtable<String, Table> Databasetables; 
   String WorkSpacePath = System.getProperty("user.dir") ;
   Hashtable<String,Integer> MetaFilePropertiesIndex = new Hashtable<String,Integer>(); 
   public Database() throws IOException{
	  
	   this.Databasetables = new Hashtable<String, Table>();
		String fileHeader = "Table Name, Column Name, Column Type, Key, Indexed, References";
		
		/*
		 * filling MetaFilePropertiesIndex hashtable with the properties in order
		 */
		
		MetaFilePropertiesIndex.put("Table Name", Integer.valueOf(0));
		MetaFilePropertiesIndex.put("Column Name", Integer.valueOf(1));
		MetaFilePropertiesIndex.put("Column Type", Integer.valueOf(2));
		MetaFilePropertiesIndex.put("Key", Integer.valueOf(3));
		MetaFilePropertiesIndex.put("Indexed", Integer.valueOf(4));
		MetaFilePropertiesIndex.put("References", Integer.valueOf(5));
		
		
		// create Metafile.csv and add the headers to it
		  File NewDirectory = new File("data");
		    NewDirectory.mkdir();
		    
		 FileWriter fileWriter = new FileWriter("data/Metafile.csv");
		  fileWriter.append(fileHeader);
		    fileWriter.append("\n");
		    fileWriter.flush();
		    fileWriter.close();
		    
		    // creating table folder to contain all tables of database
           File NewDirectory2 = new File("classes");
		    NewDirectory2.mkdir();
		    
		    File NewDirectory3 = new File("classes/Hatem 31-4562 Omar 31-5039");
		    NewDirectory3.mkdir();
		    
   }
   
   public void createTable(String strTableName,    Hashtable<String,String> htblColNameType, 
           Hashtable<String,String> htblColNameRefs, String strKeyColName)  throws DBAppException, IOException{

    Table newTable = addToMetaFile(strTableName, htblColNameType, htblColNameRefs, strKeyColName);
    
    Databasetables.put(strTableName,newTable);
    SerializeTable(newTable, strTableName);  
    
    System.out.println(strTableName+" table is Created Successfully");


}
   
   
   public Table addToMetaFile (String strstrTableName,    Hashtable<String,String> htblColNameType, 
           Hashtable<String,String> htblColNameRefs, String strKeyColName) throws FileNotFoundException, IOException {
		
    Table newTable = new  Table(htblColNameType, htblColNameRefs, strstrTableName, strKeyColName);
	   
		String NEW_LINE_SEPARATOR = "\n";
		
		File file = new  File(WorkSpacePath+"/data/Metafile.csv");
		
		if(file.exists()){

			 PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			
	
	
	 Set<String> keys = htblColNameType.keySet();
	    Iterator<String> itr = keys.iterator();
	    
	    while (itr.hasNext()) { 
	    	String ColumnName= "";
	    	String isKey="false";
	    	String ColumnType= "";
	    	String References = "null";
	    	
	    	ColumnName = itr.next();
	    	ColumnType = htblColNameType.get(ColumnName);
	    	
	    	if(htblColNameRefs.containsKey(ColumnName)){
	    		References = htblColNameRefs.get(ColumnName);
	    	}
	    	if(strKeyColName.equals(ColumnName)){
	    		isKey="true";
	    	}
	      String Column = strstrTableName+","+ColumnName+","+ColumnType+","+isKey+","+"false"+","+References;
	      writer.append(Column);
	      writer.append(NEW_LINE_SEPARATOR);
	    }	
	   writer.flush();
	   writer.close();
	   File theDir =new File(WorkSpacePath+"/classes/Hatem 31-4562 Omar 31-5039/"+strstrTableName+"");
		  theDir.mkdir();
		  
		  
		  
	    newTable.CreateNewPage();
	    
		}
		return newTable;
	}
   
   public  void insertIntoTable (String strTableName, Hashtable<String,Object> htblColNameValue) throws FileNotFoundException, ClassNotFoundException, IOException{
	  Table targetTable = null;
	   if(!Databasetables.containsKey(strTableName)){
		  System.out.println(strTableName+" table doesnot exsist");
	  }else{
		  targetTable = Databasetables.get(strTableName);
		  Set<String> keys = htblColNameValue.keySet();
		  Iterator<String> iterator = keys.iterator();
		  while(iterator.hasNext()){
			  String columnName  = iterator.next();
			  Object columnValue  = htblColNameValue.get(columnName);
		        targetTable = Databasetables.get(strTableName);
		       String columnType = targetTable.fTblColNameType.get(columnName);
		       if(columnType == null){
		    	   System.out.println("Wrong Column Name and Type");
		    	   return;
		       }
		      boolean checkValidity =  checkForValidDataType(columnValue, columnType);
		      if(!checkValidity){
		    	  System.out.println("Wrong data type for column "+columnName +" excepecting data type "+columnType);
		    	  return;
		      }
	    
		  }
		  targetTable.insertIntoPage(htblColNameValue);
		  SerializeTable(targetTable,strTableName );
		  
	  }
   }
   
   public boolean checkForValidDataType (Object value , String dataType)
   {
	  
	   
	   if(value instanceof Integer && 
			   dataType.equals("Integer") ){
		   return true;
	   }else{
		   if(value instanceof Double && dataType.equals("Double")){
			   return true;
		   }else{
			   if(value instanceof String && dataType.equals("String")){
				   return true;
			   }else{
				   if(value instanceof Date && dataType.equals("Date")){
					   return true;
				   }else{
					   if(value instanceof Boolean && dataType.equals("Boolean")){
						   return true;
					   }else{
						   return false;
					   }
				   }
			   }
		   }
	   }
	   
	
   }
   
   public Iterator SelectFromTable(String strTable,  Hashtable<String,Object> htblColNameValue,String strOperator) throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException{
	Table targetTable = Databasetables.get(strTable);
	return targetTable.SelectFromPages(htblColNameValue, strOperator);
	 
   }
   
   public void UpdateTable (String strTableName, Object strKey, Hashtable<String,Object> htblColNameValue) throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException{
	   if(!Databasetables.containsKey(strTableName)){
		   System.out.println(strTableName+" table doesnot exsist in this DataBase");
		   
	   }else{
		   Table TargetTable = Databasetables.get(strTableName);
		   TargetTable.UpdatePagesWithTheTargetRow(strKey, htblColNameValue);
	   }
	   
   }
   
   public void DeleteFromTable (String strTableName, Hashtable<String,Object> htblColNameValue, String strOperator) throws FileNotFoundException, ClassNotFoundException, IOException, DBEngineException{
	   if(!Databasetables.containsKey(strTableName)){
		   System.out.println(strTableName+" table doesnot exsist in this DataBase");
		   
	   }else{
		   Table TargetTable = Databasetables.get(strTableName);
		  TargetTable.DeleteTargetRowFromPage(htblColNameValue, strOperator);
	   }
   }
   
   public void createIndex (String strTableName, String strColName) throws IOException, ClassNotFoundException{
	   Table TargetTable = Databasetables.get(strTableName);
		 TargetTable.createIndex(strColName);
	   updateMetaFile(strTableName, strColName, "Indexed", "true" );
		System.out.println("Indexed Created successfully");
   }
   
  public  void SerializeTable (Table table ,String TableName) throws FileNotFoundException, IOException
  {
	  String FilePath = WorkSpacePath+"/classes/Hatem 31-4562 Omar 31-5039/"+TableName+"";
	  ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(FilePath+"/"+TableName+".class")));
 		oos.writeObject(table);
 		oos.flush();
  	oos.close();
  }
  
  public Table DeSerializeTable (String TableName) throws ClassNotFoundException, IOException{
	  String FilePath = WorkSpacePath+"/classes/Hatem 31-4562 Omar 31-5039/"+TableName+"/"+TableName+"";
	  ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(FilePath)));
      Table targetTable = (Table)ois.readObject();
      ois.close();
      return targetTable;
  }
   
  public void updateMetaFile (String tableName , String columnName , String propertyName , String propertyValue) throws IOException{
         System.out.println("here");
	  String NEW_LINE_SEPARATOR = "\n";

		  PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(WorkSpacePath+"/data/Metafile2.csv")));
		 BufferedReader bufferReader = new BufferedReader(new FileReader(WorkSpacePath+"/data/Metafile.csv"));
		    String line;
			while ((line = bufferReader.readLine())!= null){
			String [] properties = line.split(",");
		    if(tableName.equals(properties[0]) && columnName.equals(properties[1])){
		    	int propertyIndex = MetaFilePropertiesIndex.get(propertyName);
		    	properties[propertyIndex] = propertyValue;
		    	line = "";
		    	for(int i=0 ; i<properties.length ; i++){
		    	line+= properties[i]+",";
		    	}
		    }
		    writer.write(line.substring(0, line.length()-1) + NEW_LINE_SEPARATOR);
		    writer.flush();
		}
		
			writer.close();
			bufferReader.close();
		new File(WorkSpacePath+"/data/Metafile.csv").delete();
		new File(WorkSpacePath+"/data/Metafile2.csv").renameTo(new File(WorkSpacePath+"/data/Metafile.csv"));
	  
  }
  

   
   
	
}
