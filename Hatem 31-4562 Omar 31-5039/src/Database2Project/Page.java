package Database2Project;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.net.URL;

public class Page implements Serializable {
    String tableName ;
    ArrayList<Hashtable<String, Object>> Rows;
     int numberOfAvailableRows ;
    
	public Page (String TableName  ){
		this.tableName = TableName;
		Rows = new ArrayList<Hashtable<String ,Object>>();
		numberOfAvailableRows =0;
	  
	}
	/*
	public String toString(){
		String result = "" ; 

	     
	       
		for(int i=1; i<Rows.size() ; i++){
			String [] Row = null;
			String strRow ="";
			
				Set<String> rowkeys = Rows.get(i).keySet();
				Iterator<String> Rowiterator = rowkeys.iterator();
			
			
			
			
		while (Rowiterator.hasNext() ){
			String columnName = Rowiterator.next();
			String ColumnValue = Rows.get(i).get(columnName).toString();
			
				strRow = columnName+" = "+ColumnValue+" , "+strRow;
			}
			// to remove the space and comma from the end of each row 
			strRow = strRow.substring(0, strRow.length()-2);
		 
			result += "[ "+strRow +" ]"+"\n";
			}
		
	
		return result;
	}
	*/
	public ArrayList<Hashtable<String, Object>> getRowsArrayList (){
		return Rows;
	}
	
	
	public void setRowsArrayList(ArrayList<Hashtable<String, Object>> rows) {
		Rows = rows;
	}
	public ArrayList<String> getRows(){
		ArrayList<String> result = new ArrayList<String>();

	       
		for(int i=0; i<Rows.size() ; i++){
			
			if(Rows.get(i).get("Exists").equals(false)){
				continue;
				
			}
			
			
			String [] Row = null;
			String strRow ="";
			
				Set<String> rowkeys = Rows.get(i).keySet();
				Iterator<String> Rowiterator = rowkeys.iterator();
			
			
			
			
		while (Rowiterator.hasNext() ){
			
			
			String columnName = Rowiterator.next();
			
			//  to donot print exsist column to the user because it must be private to Database management system
			
			
			if(columnName.equals("Exists")){
				continue;
			}
			
			String ColumnValue = Rows.get(i).get(columnName).toString();
			
				strRow = columnName+" = "+ColumnValue+" , "+strRow;
			}
			// to remove the space and comma from the end of each row 
			strRow = "[ "+strRow.substring(0, strRow.length()-2)+" ]";
		    result.add(strRow);
			
			}
		
	
		return  result;
	}
	
    public void upgradePage (Hashtable<String, Object> upgradedRow , int tupleNumber){
    	//System.out.println("tuple number = "+tupleNumber);
    	//System.out.println("page = "+Rows.toString());
    	
    	/*
    	 * add 1 to tupleNumber because we have the headers of the page in the first cell
    	 */
    	
    	  Rows.set(tupleNumber+1, upgradedRow);
    	 // System.out.println("page = "+Rows.toString());
    }
	

   

public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
         ArrayList<String> a =new ArrayList<String>();
         a.add("hatem");
         a.add("Ahmed");
         a.add("Mohamed");
         a.add("Ibrahim");
         System.out.println(a.toString());
         
         a.set(1, "Glal");
         System.out.println(a.toString());
         
}
	
}
