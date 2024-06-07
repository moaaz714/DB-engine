import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Page implements Serializable {
	String pagePath;
	String pageName;
	int maxEntries;
	Object minClustKey;
	Object maxClustKey;
	public Page(String pageName, int maxEntries) {
		this.maxEntries = maxEntries;
		this.pageName = pageName;
		String directoryPath = "D:\\Semester 6\\DB\\";
		String fileName = pageName + ".class";
		this.pagePath = directoryPath + fileName;

		Vector<Tuple> vector = new Vector<>(maxEntries);
		serialize(vector);
		minClustKey=null;
		maxClustKey=null;
	}

	// serialize must OVERRIDE any existing text in the .class file

	public void serialize(Vector<Tuple> vector) {
		try {
			FileOutputStream fileOut = new FileOutputStream(pagePath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(vector);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}


	public boolean isEmpty() {

		Vector<Tuple> vector = deserializeAll();
		if (vector.size()==0)
			return true;

		return false;
	}


	public void removeTuple(int rowNum) {
		Vector v= this.deserializeAll();
		v.remove(rowNum);
		this.serialize(v);
	}

	// This method returns all the tuples of the page in a string where each tuple
	// is separated with a "%" sign ,
	// ie "ahmed,19,2.2,%ahmed,19,2.22,%"
	// It uses the method deserializeAll() to get all the tuples in an array list,
	// then loop on each tuple and add it to the deserialized string
	public String toString() {
		Vector<Tuple> tupleList = deserializeAll();
		String deserializedString = "";
		for (Tuple tuple : tupleList) {
			deserializedString += tuple.toString() + "%";
		}
		return deserializedString;
	}

	// This method Deserialzes one tuple only and returns it
	// parameter row: It is the row in the page with the targeted tuple
	public Tuple deserializeTuple(int row) {
		Tuple tuple = null;
		try {
			FileInputStream fileIn = new FileInputStream(pagePath);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			for (int i = 0; i < row; i++) {
				try {
					tuple = (Tuple) in.readObject();
				} catch (Exception e) {
					break;
				}
			}
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		}

		return tuple;
	}

	// This method deserializes all the tuples in the page and returns them in an
	// array list "tupleList"
	public Vector<Tuple> deserializeAll() {
		Vector<Tuple> tupleList = null;
		try {
			FileInputStream fileIn = new FileInputStream(pagePath);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			try {
				tupleList = (Vector<Tuple>) in.readObject();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		}
		return tupleList;
	}

	public String getPath() {
		return pagePath;
	}

	public String getPageName() {
		return pageName;
	}

	public boolean isFull() {

		Vector<Tuple> vector = deserializeAll();
		if (this.maxEntries == vector.size())
			return true;

		return false;
	}

	

	public ArrayList<String> getColumnNamesFromCsv(String tableName) {

		ArrayList<String> names = new ArrayList<>();
		String csvFile = DBApp.metaData; // Path to CSV file
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			String line;

			// this loops over all lines
			while ((line = br.readLine()) != null) {
				// Splitting the line by comma to get individual fields
				String[] fields = line.split(",");

				// Process each field
				if (fields[0] == tableName) {
					names.add(fields[1]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return names;
	}

	
	
	
	public int findInsertionPosition(Object value,String tableName) throws DBAppException {
		
		//this has it as a given that if the page that i want to insert into is the last , it is not full
		
		Vector<Tuple> vector =this.deserializeAll();
		int primaryKeyPosition=DBApp.getPrimaryKeyPosition(tableName);
		return binarySearchInsertionHelper(0, vector.size()-1,  vector,  primaryKeyPosition,  value);
	}
	public int binarySearchInsertionHelper(int start, int end, Vector<Tuple> vector, int primaryKeyPosition, Object value) throws DBAppException {

		if(end<start)
			return start;

		if (value instanceof String) {

			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				String inputValueString = (String)(value);
				String tupleValueString = (String)valueFromTuple;
				if (inputValueString.compareTo(tupleValueString) > 0) {
					return start + 1;

				} else if(inputValueString.compareTo(tupleValueString) < 0) {

					return start;
				}else {
					//duplicate clust key
					throw new DBAppException("duplicate primary key");
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			String inputValueString = (String)(value);
			String tupleValueString = (String)valueFromTuple;

			if (inputValueString.compareTo(tupleValueString) == 0) {
				// i found a match clusterring key can not be duplicated
				throw new DBAppException("duplicate primary key");

			} else if (inputValueString.compareTo(tupleValueString) > 0) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearchInsertionHelper(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearchInsertionHelper(start, mid - 1, vector, primaryKeyPosition, value);

			}
		}else if(value instanceof Double) {
			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				Double inputValueString = (Double)(value);
				Double tupleValueString = (Double)valueFromTuple;
				if (inputValueString>tupleValueString) {
					return start + 1;

				} else if(inputValueString<tupleValueString){

					return start;
				}else {
					//duplicate clust key
					throw new DBAppException("duplicate primary key");
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			Double inputValueString = (Double)value;
			Double tupleValueString = (Double)valueFromTuple;

			if (inputValueString==tupleValueString) {
				// i found a match clusterring key can not be duplicated
				throw new DBAppException("duplicate primary key");

			} else if (inputValueString>tupleValueString) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearchInsertionHelper(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearchInsertionHelper(start, mid - 1, vector, primaryKeyPosition, value);

			}
			
		}else {
			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				Integer inputValueString = (Integer)value;
				Integer tupleValueString = (Integer)valueFromTuple;
				if (inputValueString>tupleValueString) {
					return start + 1;

				} else if(inputValueString<tupleValueString){

					return start;
				}else {
					//duplicate primary key
					throw new DBAppException("duplicate primary key");
					
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			Integer inputValueString = (Integer)value;
			Integer tupleValueString = (Integer)valueFromTuple;
			
			if (inputValueString==tupleValueString) {
				// i found a match clusterring key can not be duplicated
				throw new DBAppException("duplicate primary key");

			} else if (inputValueString>tupleValueString) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearchInsertionHelper(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearchInsertionHelper(start, mid - 1, vector, primaryKeyPosition, value);

			}
			
			
		}

	}
	
	public int findSelection(Object value,String tableName) throws DBAppException {
		Vector<Tuple> vector =this.deserializeAll();
		int primaryKeyPosition=DBApp.getPrimaryKeyPosition(tableName);
		return binarySearch(0, vector.size()-1,  vector,  primaryKeyPosition,  value);
		
	} 
	public int binarySearch(int start, int end, Vector<Tuple> vector, int primaryKeyPosition, Object value) {

		

		if (value instanceof String) {

			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				String inputValueString = (String)(value);
				String tupleValueString = (String)valueFromTuple;
				if (inputValueString.compareTo(tupleValueString) > 0) {
					return start + 1;

				} else {

					return start;
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			String inputValueString = (String)(value);
			String tupleValueString = (String)valueFromTuple;

			if (inputValueString.compareTo(tupleValueString) == 0) {
				// i found a match , it works to insert in the same spot as the already existing
				// value will be shifted to the right once
				// then the new value will be inserted , which works.
				return mid;

			} else if (inputValueString.compareTo(tupleValueString) > 0) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearch(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearch(start, mid - 1, vector, primaryKeyPosition, value);

			}
		}else if(value instanceof Double) {
			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				Double inputValueString = (Double)(value);
				Double tupleValueString = (Double)valueFromTuple;
				if (inputValueString>tupleValueString) {
					return start + 1;

				} else {

					return start;
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			Double inputValueString = (Double)value;
			Double tupleValueString = (Double)valueFromTuple;

			if (inputValueString==tupleValueString) {
				// i found a match , it works to insert in the same spot as the already existing
				// value will be shifted to the right once
				// then the new value will be inserted , which works.
				return mid;

			} else if (inputValueString>tupleValueString) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearch(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearch(start, mid - 1, vector, primaryKeyPosition, value);

			}
			
		}else {
			if (end == start) {
				// check if the value needs to be to inserted at start (shift value at position
				// start to the right)
				// which means en value at position start is more than the value i'm trying to
				// insert ,
				// walla ha7tag a insert b3dha
				Tuple tuple = vector.get(start);
				Object valueFromTuple = tuple.values.get(primaryKeyPosition);
				Integer inputValueString = (Integer)value;
				Integer tupleValueString = (Integer)valueFromTuple;
				if (inputValueString>tupleValueString) {
					return start + 1;

				} else {

					return start;
				}

			}

			int mid = start + ((end - start) / 2);

			Tuple tuple = vector.get(mid);
			Object valueFromTuple = tuple.values.get(primaryKeyPosition);
			Integer inputValueString = (Integer)value;
			Integer tupleValueString = (Integer)valueFromTuple;
			
			if (inputValueString==tupleValueString) {
				// i found a match , it works to insert in the same spot as the already existing
				// value will be shifted to the right once
				// then the new value will be inserted , which works.
				return mid;

			} else if (inputValueString>tupleValueString) {
				// inputValueString should be inserted somewhere to the right of
				// tupleValueString,
				// the position still needs to be found
				// tupleValueString is at position mid
				return binarySearch(mid + 1, end, vector, primaryKeyPosition, value);

			} else {
				// inputValueString should be inserted somewhere to the left of
				// tupleValueString,
				// the position still needs to be found
				return binarySearch(start, mid - 1, vector, primaryKeyPosition, value);

			}
			
			
		}

	}

	public static void main(String[] args) throws DBAppException {
//		Page p = new Page("Student", 200);
//		ArrayList<Object> values = new ArrayList<Object>();
//		values.add("Ahmed");
//		values.add(20);
//		values.add(2.2);
//
//		Tuple tuple = new Tuple(values);
//		p.addEntry(tuple);
//
//		Vector<Tuple> tuples = p.deserializeAll();
//		for (Tuple t : tuples) {
//			System.out.println(t.toString());
//		}
//
//		ArrayList<Object> values2 = new ArrayList<Object>();
//		values2.add("Saleh");
//		values2.add(20);
//		values2.add(2.1);
//
//		Tuple tuple2 = new Tuple(values2);
//		p.addEntry(tuple2);
//
//		Vector<Tuple> tuples2 = p.deserializeAll();
//		for (Tuple t : tuples2) {
//			System.out.println(t.toString());
//		}
//		String[] test = p.toString().split("%");
//		System.out.println(test[0].split(",")[1]);

	}

}
