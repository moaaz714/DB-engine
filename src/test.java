import java.io.*;
import java.util.*;


public class test implements Serializable{
	
	public static void main(String[]args) {
		Page p1=new Page("TestPage1",5);
		Page p2=new Page("TestPage2",5);

		
		Vector<Tuple> vector= p1.deserializeAll();
		Vector<Tuple> vector2= p2.deserializeAll();

		ArrayList<Object> arr=new ArrayList();
		arr.add(0);
		arr.add("ahmed");
		Tuple t=new Tuple(arr);
		
		ArrayList<Object> arr2=new ArrayList();
		arr2.add(2);
		arr2.add("ahmed");
		Tuple t2=new Tuple(arr2);
		
		ArrayList<Object> arr4=new ArrayList();
		arr4.add(4);
		arr4.add("ahmed");
		Tuple t4=new Tuple(arr4);
		

		ArrayList<Object> arr6=new ArrayList();
		arr6.add(6);
		arr6.add("ahmed");
		Tuple t6=new Tuple(arr6);

		ArrayList<Object> arr8=new ArrayList();
		arr8.add(8);
		arr8.add("ahmed");
		Tuple t8=new Tuple(arr8);

		ArrayList<Object> arr10=new ArrayList();
		arr10.add(10);
		arr10.add("ahmed");
		Tuple t10=new Tuple(arr10);

		ArrayList<Object> arr12=new ArrayList();
		arr12.add(12);
		arr12.add("ahmed");
		Tuple t12=new Tuple(arr12);

		ArrayList<Object> arr14=new ArrayList();
		arr14.add(14);
		arr14.add("ahmed");
		Tuple t14=new Tuple(arr14);

		ArrayList<Object> arr16=new ArrayList();
		arr16.add(16);
		arr16.add("ahmed");
		Tuple t16=new Tuple(arr16);

		ArrayList<Object> arr18=new ArrayList();
		arr18.add(18);
		arr18.add("ahmed");
		Tuple t18=new Tuple(arr18);
		
		vector.add(t);
		vector.add(t2);
		vector.add(t4);
		vector.add(t6);
		vector.add(t8);
		vector2.add(t10);
		vector2.add(t12);
		vector2.add(t14);
		vector2.add(t16);
		vector2.add(t18);
	

		
		
		
		
		
		p1.serialize(vector);
		p2.serialize(vector2);
		Table table=new Table("table",3);
		table.addPage(p1);
		table.addPage(p2);
		
		
		System.out.println("answer is "+p1.binarySearch(0 ,9 ,vector,0,12 ));

	}
}
