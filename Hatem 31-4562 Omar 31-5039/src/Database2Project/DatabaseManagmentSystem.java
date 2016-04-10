package Database2Project;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


public class DatabaseManagmentSystem {





public void CreateDatabase () throws IOException{
	Database newDatabase = new Database();
	SerializeDatabase( newDatabase);
	System.out.println(" Database created successfully");
}

public void SerializeDatabase (Database newDatabase) throws IOException{
	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("MyDB.class")));
		oos.writeObject(newDatabase);
	oos.close();
}

public Database deserializeDatabase (String DbName) throws ClassNotFoundException, IOException{
	ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(DbName+".class")));
    Database targetDatabase = (Database)ois.readObject();
    ois.close();
    return targetDatabase;
}

}
