package Database2Project;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Iterator;

import Database2Project.Table;
import Exceptions.DBAppException;
import Exceptions.DBEngineException;



public class DBApp {
Database myDataBase;
String WorkSpacePath = System.getProperty("user.dir") ;
	
	public void init( ) throws ClassNotFoundException, IOException{
	   myDataBase=deserializeDatabase();
    }
	
	public Database CreateDatabase () throws IOException{
		Database newDatabase = new Database();
		SerializeDatabase( newDatabase);
		System.out.println(" Database created successfully");
		return newDatabase;
	}

	public void SerializeDatabase (Database newDatabase) throws IOException{
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(WorkSpacePath+"/data/MyDataBase.class")));
			oos.writeObject(newDatabase);
			oos.flush();
		oos.close();
	}

	public Database deserializeDatabase () throws ClassNotFoundException, IOException{
		Database targetDatabase = null;
		
		File file = new File(WorkSpacePath+"/data/MyDataBase.class");
		if(file.exists()){
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(WorkSpacePath+"/data/MyDataBase.class")));
	     targetDatabase = (Database)ois.readObject();
	    ois.close();

	    
		}else{
			targetDatabase = CreateDatabase();
		}
	    return targetDatabase;
	}


	
    public void createTable(String strTableName,    Hashtable<String,String> htblColNameType, 
                            Hashtable<String,String> htblColNameRefs, String strKeyColName)  throws DBAppException, IOException, ClassNotFoundException{
    
    	myDataBase.createTable(strTableName, htblColNameType, htblColNameRefs, strKeyColName);
    	createIndex(strTableName, strKeyColName);
    	SerializeDatabase(myDataBase);
    	
    	
    }

    public void createIndex(String strTableName, String strColName)  throws DBAppException, IOException, ClassNotFoundException{
        myDataBase.createIndex(strTableName, strColName);
        SerializeDatabase(myDataBase);
    }

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue)  throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException{
    myDataBase.insertIntoTable(strTableName, htblColNameValue);
    SerializeDatabase(myDataBase);
    }

    public void updateTable(String strTableName, Object strKey,
                            Hashtable<String,Object> htblColNameValue)  throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException{
    try{
    	myDataBase.UpdateTable(strTableName, strKey, htblColNameValue);
        SerializeDatabase(myDataBase);
    }
    catch(DBEngineException e){
    	System.out.println(e.getMessage());
    }
    
    }


    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue, 
                                String strOperator) throws DBEngineException, FileNotFoundException, ClassNotFoundException, IOException{
        try{
        	myDataBase.DeleteFromTable(strTableName, htblColNameValue, strOperator);
            SerializeDatabase(myDataBase);
        }
        catch(DBEngineException e){
        	System.out.println(e.getMessage());
        }
    
    }
		
    public Iterator selectFromTable(String strTable,  Hashtable<String,Object> htblColNameValue, 
                                    String strOperator) throws DBEngineException, FileNotFoundException, ClassNotFoundException, IOException{
      try{
    	  return myDataBase.SelectFromTable(strTable, htblColNameValue, strOperator);
      }
      catch(DBEngineException e){
      	System.out.println(e.getMessage());
      	throw e;
      }
   
    }

    public static void main(String [] args) throws DBAppException, DBEngineException, FileNotFoundException, ClassNotFoundException, IOException {
		// creat a new DBApp
		DBApp myDB = new DBApp();

		// initialize it
		myDB.init();

//	
		// creating table "Faculty"
     
				Hashtable<String, String> fTblColNameType = new Hashtable<String, String>();
				fTblColNameType.put("ID", "Integer");
				fTblColNameType.put("Name", "String");

				Hashtable<String, String> fTblColNameRefs = new Hashtable<String, String>();

				myDB.createTable("Faculty", fTblColNameType, fTblColNameRefs, "ID");

				// creating table "Major"

				Hashtable<String, String> mTblColNameType = new Hashtable<String, String>();
				mTblColNameType.put("ID", "Integer");
				mTblColNameType.put("Name", "String");
				mTblColNameType.put("Faculty_ID", "Integer");

				Hashtable<String, String> mTblColNameRefs = new Hashtable<String, String>();
				mTblColNameRefs.put("Faculty_ID", "Faculty.ID");

				myDB.createTable("Major", mTblColNameType, mTblColNameRefs, "ID");

				// creating table "Course"

				Hashtable<String, String> coTblColNameType = new Hashtable<String, String>();
				coTblColNameType.put("ID", "Integer");
				coTblColNameType.put("Name", "String");
				coTblColNameType.put("Code", "String");
				coTblColNameType.put("Hours", "Integer");
				coTblColNameType.put("Semester", "Integer");
				coTblColNameType.put("Major_ID", "Integer");

				Hashtable<String, String> coTblColNameRefs = new Hashtable<String, String>();
				coTblColNameRefs.put("Major_ID", "Major.ID");

				myDB.createTable("Course", coTblColNameType, coTblColNameRefs, "ID");

				// creating table "Student"

				Hashtable<String, String> stTblColNameType = new Hashtable<String, String>();
				stTblColNameType.put("ID", "Integer");
				stTblColNameType.put("First_Name", "String");
				stTblColNameType.put("Last_Name", "String");
				stTblColNameType.put("GPA", "Double");
				stTblColNameType.put("Age", "Integer");

				Hashtable<String, String> stTblColNameRefs = new Hashtable<String, String>();

				myDB.createTable("Student", stTblColNameType, stTblColNameRefs, "ID");

				// creating table "Student in Course"

				Hashtable<String, String> scTblColNameType = new Hashtable<String, String>();
				scTblColNameType.put("ID", "Integer");
				scTblColNameType.put("Student_ID", "Integer");
				scTblColNameType.put("Course_ID", "Integer");

				Hashtable<String, String> scTblColNameRefs = new Hashtable<String, String>();
				scTblColNameRefs.put("Student_ID", "Student.ID");
				scTblColNameRefs.put("Course_ID", "Course.ID");

				myDB.createTable("Student_in_Course", scTblColNameType, scTblColNameRefs, "ID");

				// insert in table "Faculty"

				Hashtable<String,Object> ftblColNameValue1 = new Hashtable<String,Object>();
				ftblColNameValue1.put("ID", Integer.valueOf( "0" ) );
				ftblColNameValue1.put("Name", "Media Engineering and Technology");
				myDB.insertIntoTable("Faculty", ftblColNameValue1);

				Hashtable<String,Object> ftblColNameValue2 = new Hashtable<String,Object>();
				ftblColNameValue2.put("ID", Integer.valueOf( "1" ) );
				ftblColNameValue2.put("Name", "Management Technology");
				myDB.insertIntoTable("Faculty", ftblColNameValue2);

				for(int i=0;i<1000;i++)
				{
					Hashtable<String,Object> ftblColNameValueI = new Hashtable<String,Object>();
					ftblColNameValueI.put("ID", Integer.valueOf( (""+(i+2)) ) );
					ftblColNameValueI.put("Name", "f"+(i+2));
					myDB.insertIntoTable("Faculty", ftblColNameValueI);
				}

				// insert in table "Major"

				Hashtable<String,Object> mtblColNameValue1 = new Hashtable<String,Object>();
				mtblColNameValue1.put("ID", Integer.valueOf( "0" ) );
				mtblColNameValue1.put("Name", "Computer Science & Engineering");
				mtblColNameValue1.put("Faculty_ID", Integer.valueOf( "1" ) );
				myDB.insertIntoTable("Major", mtblColNameValue1);

				Hashtable<String,Object> mtblColNameValue2 = new Hashtable<String,Object>();
				mtblColNameValue2.put("ID", Integer.valueOf( "1" ));
				mtblColNameValue2.put("Name", "Business Informatics");
				mtblColNameValue2.put("Faculty_ID", Integer.valueOf( "2" ));
				myDB.insertIntoTable("Major", mtblColNameValue2);

				for(int i=0;i<1000;i++)
				{
					Hashtable<String,Object> mtblColNameValueI = new Hashtable<String,Object>();
					mtblColNameValueI.put("ID", Integer.valueOf( (""+(i+2) ) ));
					mtblColNameValueI.put("Name", "m"+(i+2));
					mtblColNameValueI.put("Faculty_ID", Integer.valueOf( (""+(i+2) ) ));
					myDB.insertIntoTable("Major", mtblColNameValueI);
				}


				// insert in table "Course"

				Hashtable<String,Object> ctblColNameValue1 = new Hashtable<String,Object>();
				ctblColNameValue1.put("ID", Integer.valueOf( "0" ) );
				ctblColNameValue1.put("Name", "Data Bases II");
				ctblColNameValue1.put("Code", "CSEN 604");
				ctblColNameValue1.put("Hours", Integer.valueOf( "4" ));
				ctblColNameValue1.put("Semester", Integer.valueOf( "6" ));
				ctblColNameValue1.put("Major_ID", Integer.valueOf( "1" ));
				myDB.insertIntoTable("Course", mtblColNameValue1);

				Hashtable<String,Object> ctblColNameValue2 = new Hashtable<String,Object>();
				ctblColNameValue2.put("ID", Integer.valueOf( "1" ) );
				ctblColNameValue2.put("Name", "Data Bases II");
				ctblColNameValue2.put("Code", "CSEN 604");
				ctblColNameValue2.put("Hours", Integer.valueOf( "4" ) );
				ctblColNameValue2.put("Semester", Integer.valueOf( "6" ) );
				ctblColNameValue2.put("Major_ID", Integer.valueOf( "2" ) );
				myDB.insertIntoTable("Course", mtblColNameValue2);

				for(int i=0;i<1000;i++)
				{
					Hashtable<String,Object> ctblColNameValueI = new Hashtable<String,Object>();
					ctblColNameValueI.put("ID", Integer.valueOf( ( ""+(i+2) )));
					ctblColNameValueI.put("Name", "c"+(i+2));
					ctblColNameValueI.put("Code", "co "+(i+2));
					ctblColNameValueI.put("Hours", Integer.valueOf( "4" ) );
					ctblColNameValueI.put("Semester", Integer.valueOf( "6" ) );
					ctblColNameValueI.put("Major_ID", Integer.valueOf( ( ""+(i+2) )));
					myDB.insertIntoTable("Course", ctblColNameValueI);
				}

				// insert in table "Student"

				for(int i=0;i<1000;i++)
				{
					Hashtable<String,Object> sttblColNameValueI = new Hashtable<String,Object>();
					sttblColNameValueI.put("ID", Integer.valueOf( ( ""+i ) ) );
					sttblColNameValueI.put("First_Name", "FN"+i);
					sttblColNameValueI.put("Last_Name", "LN"+i);
					sttblColNameValueI.put("GPA", Double.valueOf( "0.7" ) ) ;
					sttblColNameValueI.put("Age", Integer.valueOf( "20" ) );
					myDB.insertIntoTable("Student", sttblColNameValueI);
				//changed it to student instead of course
				}

//
//		// selecting


		  Hashtable<String,Object> stblColNameValue = new Hashtable<String,Object>();
		stblColNameValue.put("ID", Integer.valueOf( "400" ) );
		//stblColNameValue.put("Age", Integer.valueOf( "20" ) );

		long startTime = System.currentTimeMillis();
		Iterator myIt = myDB.selectFromTable("Student", stblColNameValue,"");
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		while(myIt.hasNext()) {
			System.out.println(myIt.next());
		}

		// feel free to add more tests
//		
//        Hashtable<String,Object> stblColNameValue3 = new Hashtable<String,Object>();
//		stblColNameValue3.put("Name", "m7");
//		stblColNameValue3.put("Faculty_ID", Integer.valueOf( "7" ) );
//
//      long startTime2 = System.currentTimeMillis();
//		Iterator myIt2 = myDB.selectFromTable("Major", stblColNameValue3,"AND");
//		long endTime2   = System.currentTimeMillis();
//		long totalTime2 = endTime2 - startTime2;
//		System.out.println(totalTime2);
//		while(myIt2.hasNext()) {
//			System.out.println(myIt2.next());
//		}
//		
//		
//		// updating row with ID = 994 to have different age
//		
//		Hashtable<String,Object> stblColNameValue1 = new Hashtable<String,Object>();
//		stblColNameValue1.put("GPA", Double.valueOf( "2.9" ) );
//		stblColNameValue1.put("Age", Integer.valueOf( "21" ));
//		myDB.updateTable("Student",Integer.valueOf("994"),stblColNameValue1);
//		
//	// select row with ID = 994 (indexed) and Age = 21
//		Hashtable<String,Object> stblColNameValueTag = new Hashtable<String,Object>();
//		stblColNameValueTag.put("ID", Integer.valueOf( "994" ));
//		stblColNameValueTag.put("Age", Integer.valueOf( "21" ) );
//		
//		long startTime3 = System.currentTimeMillis();
//		Iterator myIt3 = myDB.selectFromTable("Student", stblColNameValueTag,"AND");
//		long endTime3   = System.currentTimeMillis();
//		long totalTime3 = endTime3 - startTime3;
//		System.out.println(totalTime3);
//		while(myIt3.hasNext()) {
//			System.out.println(myIt3.next());
//		}
////      // select the same row as the above query but with GPA and Age both not indexed
//		Hashtable<String,Object> stblColNameValueTag1 = new Hashtable<String,Object>();
//		stblColNameValueTag1.put("GPA", Double.valueOf( "2.9" ) );
//		stblColNameValueTag1.put("Age", Integer.valueOf( "21" ) );
//		
//		long startTime6 = System.currentTimeMillis();
//		Iterator myIt6= myDB.selectFromTable("Student", stblColNameValueTag1,"AND");
//		long endTime6  = System.currentTimeMillis();
//		long totalTime6 = endTime6 - startTime6;
//		System.out.println(totalTime6);
//		while(myIt6.hasNext()) {
//			System.out.println(myIt6.next());
//		}
////		
////		// select a row by an indexed column 
//		Hashtable<String,Object> stblColNameValueTag3 = new Hashtable<String,Object>();
//		stblColNameValueTag3.put("ID", Integer.valueOf( "994" ));
//		
//		
//		long startTime4 = System.currentTimeMillis();
//		Iterator myIt4 = myDB.selectFromTable("Student", stblColNameValueTag3,"");
//		long endTime4   = System.currentTimeMillis();
//		long totalTime4 = endTime4 - startTime4;
//		System.out.println(totalTime4);
//		while(myIt4.hasNext()) {
//			System.out.println(myIt4.next());
//		}
		
				// select a row by a non indexed column 
				Hashtable<String,Object> stblColNameValueTag5 = new Hashtable<String,Object>();
				stblColNameValueTag5.put("Age", Integer.valueOf( "21" ));
				
				
				long startTime5 = System.currentTimeMillis();
				Iterator myIt5 = myDB.selectFromTable("Student", stblColNameValueTag5,"");
				long endTime5   = System.currentTimeMillis();
				long totalTime5 = endTime5 - startTime5;
				System.out.println(totalTime5);
				while(myIt5.hasNext()) {
					System.out.println(myIt5.next());
				}
		
//				
//				// updating row with ID = 995 to have different age
//			
//				Hashtable<String,Object> stblColNameValue2 = new Hashtable<String,Object>();
//				stblColNameValue2.put("GPA", Double.valueOf( "2.9" ) );
//				stblColNameValue2.put("Age", Integer.valueOf( "21" ));
//				myDB.updateTable("Student",Integer.valueOf("150"),stblColNameValue2);
//				
//				// select row with ID  (indexed) OR Age = 21
//				Hashtable<String,Object> stblColNameValueTag7= new Hashtable<String,Object>();
//				stblColNameValueTag7.put("ID", Integer.valueOf( "150" ));
//				stblColNameValueTag7.put("Age", Integer.valueOf( "21" ) );
//				
//				long startTime7 = System.currentTimeMillis();
//				Iterator myIt7= myDB.selectFromTable("Student", stblColNameValueTag7,"AND");
//				long endTime7   = System.currentTimeMillis();
//				long totalTime7 = endTime7 - startTime7;
//				System.out.println(totalTime7);
//				while(myIt7.hasNext()) {
//					System.out.println(myIt7.next());
//				}
//				
//
////				/*
////				 *  select row with or Operator on with two conditions on indexed Columns
////				 *  to check the efficiency of the btree
////				 *  
////				 */
////				/*
//				Hashtable<String,Object> stblColNameValueTag9= new Hashtable<String,Object>();
//				stblColNameValueTag9.put("ID", Integer.valueOf( "993" ));
//				stblColNameValueTag9.put("ID", Integer.valueOf( "996" ) );
//				
//				long startTime9 = System.currentTimeMillis();
//				Iterator myIt9= myDB.selectFromTable("Student", stblColNameValueTag9,"OR");
//				long endTime9   = System.currentTimeMillis();
//				long totalTime9 = endTime9 - startTime9;
//				System.out.println(totalTime9);
//				while(myIt9.hasNext()) {
//					System.out.println(myIt9.next());
//				}
////				*/
////				
////		   /*
////		    * updating record with id = 993 by setting the Age to 22 to be different 
////		    * in order to be abl to select rows by or operator without using any indexed columns
////		    * in the where closure so we can determine how efficient is the Btree
////		    */
////				
////				
////				// select row with ID = 994 (indexed) OR Age = 21
//				Hashtable<String,Object> stblColNameValueTag8= new Hashtable<String,Object>();
//				//stblColNameValueTag8.put("GPA", Double.valueOf( "1.1" ));
//				stblColNameValueTag8.put("ID", Integer.valueOf( "180" ) );
//				stblColNameValueTag8.put("Age", Integer.valueOf( "21" ) );
//				long startTime8 = System.currentTimeMillis();
//				Iterator myIt8= myDB.selectFromTable("Student", stblColNameValueTag8,"OR");
//				long endTime8 = System.currentTimeMillis();
//				long totalTime8 = endTime8 - startTime8;
//				System.out.println(totalTime8);
//				while(myIt8.hasNext()) {
//					System.out.println(myIt8.next());
//				}
////	
//////				// Deleting
//	
//        Hashtable<String,Object> stblColNameValueTag10 = new Hashtable<String,Object>();
//		//stblColNameValueTag10.put("GPA", Double.valueOf( "2.9" ) );
//		stblColNameValueTag10.put("First_Name", "FN182" );
//		stblColNameValueTag10.put("Age", Integer.valueOf( "20" ) );
//        long startTime10 = System.currentTimeMillis();
//        myDB.deleteFromTable("Student", stblColNameValueTag10,"AND");
//		long endTime10  = System.currentTimeMillis();
//		long totalTime10 = endTime10 - startTime10;
//		System.out.println(totalTime10);
//		
//
//		
//		System.out.println("After Deleteing");
//		
//////		// select rows after Deleting
//		Hashtable<String,Object> stblColNameValueTag11= new Hashtable<String,Object>();
//		//stblColNameValueTag11.put("GPA", Double.valueOf( "1.1" ));
//		stblColNameValueTag11.put("ID", Integer.valueOf( "180" ) );
//		stblColNameValueTag11.put("Age", Integer.valueOf( "21" ) );
//		
//		long startTime11 = System.currentTimeMillis();
//		Iterator myIt11= myDB.selectFromTable("Student", stblColNameValueTag11,"AND");
//		long endTime11 = System.currentTimeMillis();
//		long totalTime11 = endTime11 - startTime11;
//		System.out.println(totalTime11);
//		while(myIt11.hasNext()) {
//			System.out.println(myIt11.next());
//		}
//		
//		
//		/*
//		 * create an index on GPA column
//		 */
//		
//     	myDB.createIndex("Student", "GPA");
//		
//		/*
//		 * Selecting after creating a new index
//		 */
//		
//		
//		Hashtable<String,Object> stblColNameValue20 = new Hashtable<String,Object>();
//		stblColNameValue20.put("GPA", Double.valueOf( "1.1" ) );
//		stblColNameValue20.put("Age", Integer.valueOf( "21" ));
//		myDB.updateTable("Student",Integer.valueOf("150"),stblColNameValue20);
//		
//		
//		Hashtable<String,Object> stblColNameValueTag14= new Hashtable<String,Object>();
//		stblColNameValueTag14.put("GPA", Double.valueOf( "1.1" ));
//		stblColNameValueTag14.put("ID", Integer.valueOf( "150" ) );
//		//stblColNameValueTag8.put("Age", Integer.valueOf( "21" ) );
//		long startTime14 = System.currentTimeMillis();
//		Iterator myIt14= myDB.selectFromTable("Student", stblColNameValueTag14,"AND");
//		long endTime14 = System.currentTimeMillis();
//		long totalTime14 = endTime14 - startTime14;
//		System.out.println(totalTime14);
//		while(myIt14.hasNext()) {
//			System.out.println(myIt14.next());
//		}
//		
//		
//		Hashtable<String,Object> stblColNameValueTag13= new Hashtable<String,Object>();
//		stblColNameValueTag13.put("GPA", Double.valueOf( "1.1" ));
//		//stblColNameValueTag13.put("ID", Integer.valueOf( "150" ) );
//		//stblColNameValueTag8.put("Age", Integer.valueOf( "21" ) );
//		long startTime13 = System.currentTimeMillis();
//		Iterator myIt13= myDB.selectFromTable("Student", stblColNameValueTag13,"");
//		long endTime13 = System.currentTimeMillis();
//		long totalTime13 = endTime13 - startTime13;
//		System.out.println(totalTime13);
//		while(myIt13.hasNext()) {
//			System.out.println(myIt13.next());
//		}
//		
		
		
		
		
    }
}

