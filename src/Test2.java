import java.util.ArrayList;
import java.util.Vector;

public class Test2 {
	
	public Test2() {}
	public void p(Object x) {
		System.out.println(x);

	}
	
	public static void main(String[] args) {
		bplustree B = new bplustree(4);
		B.insert(3, "Student1");
		B.insert(3, "Student2");
		B.insert(7, "Student1");
		System.out.println(B.search(9)==null);
		System.out.println(B.search(3));
		
		Vector<Object> v = new Vector();
		Object c =3;
		v.add(c);
		System.out.println(v);
		v.remove(c);
		System.out.println(v);
		

			
	}
}
