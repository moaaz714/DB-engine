import java.lang.*;
import java.util.*;
import java.io.*;

public class DictionaryPair implements Comparable<DictionaryPair>,Serializable{
		Object key;
		Vector<String> value=new Vector<String>();

		/**
		 * Constructor
		 * @param key: the key of the key-value pair
		 * @param value: the value of the key-value pair
		 */
		public DictionaryPair(Object key, String value) {
			this.key = key;
			this.value.add(value);
		}

		/**
		 * This is a method that allows comparisons to take place between
		 * DictionaryPair objects in order to sort them later on
		 * @param o
		 * @return
		 */
		@Override
		public int compareTo(DictionaryPair o) {
			if(key.getClass().getSimpleName().equals("String")) {
				String tempKey=(String)key;
				String tempOKey=(String)o.key;
				if(tempKey.compareTo(tempOKey)==0) {
					return 0;
				}else if(tempKey.compareTo(tempOKey)>0) {
					return 1;
				}else {
					return -1;
				}
			}else if (key.getClass().getSimpleName().equals("Integer")){
				int tempKey= (int)key;
				int tempOKey= (int)key;
				if(tempKey==tempOKey) {
					return 0;
				}else if(tempKey>tempOKey) {
					return 1;
				}else {
					return -1;
				}
			}else {
				double tempKey= (double)key;
				double tempOKey= (double)key;
				if(tempKey==tempOKey) {
					return 0;
				}else if(tempKey>tempOKey) {
					return 1;
				}else {
					return -1;
				}
			}
		}
	}