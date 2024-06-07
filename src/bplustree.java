import java.lang.*;
import java.util.*;
import java.io.*;

public class bplustree implements Serializable {
	int m;
	InternalNode root;
	LeafNode firstLeaf;

	/*~~~~~~~~~~~~~~~~ HELPER FUNCTIONS ~~~~~~~~~~~~~~~~*/

	/**
	 * This method performs a standard binary search on a sorted
	 * DictionaryPair[] and returns the index of the dictionary pair
	 * with target key t if found. Otherwise, this method returns a negative
	 * value.
	 * @param dps: list of dictionary pairs sorted by key within leaf node
	 * @param t: target key value of dictionary pair being searched for
	 * @return index of the target value if found, else a negative value
	 */
	private int binarySearch(DictionaryPair[] dps, int numPairs, Object t) {
		Comparator<DictionaryPair> c = new Comparator<DictionaryPair>() {
			@Override
			public int compare(DictionaryPair o1, DictionaryPair o2) {
				return compareTo(o1.key,o2.key);
			}
		};
		return Arrays.binarySearch(dps, 0, numPairs, new DictionaryPair(t, ""), c);
	}

	/**
	 * This method starts at the root of the B+ tree and traverses down the
	 * tree via key comparisons to the corresponding leaf node that holds 'key'
	 * within its dictionary.
	 * @param key: the unique key that lies within the dictionary of a LeafNode object
	 * @return the LeafNode object that contains the key within its dictionary
	 */
	private LeafNode findLeafNode(Object key) {

		// Initialize keys and index variable
		Object[] keys = this.root.keys;
		int i;

		// Find next node on path to appropriate leaf node
		for (i = 0; i < this.root.degree - 1; i++) {
			if (compareTo(key,keys[i])<0) { break; }
		}

		/* Return node if it is a LeafNode object,
		   otherwise repeat the search function a level down */
		Node child = this.root.childPointers[i];
		if (child instanceof LeafNode) {
			return (LeafNode)child;
		} else {
			return findLeafNode((InternalNode)child, key);
		}
	}

	private LeafNode findLeafNode(InternalNode node, Object key) {

		// Initialize keys and index variable
		Object[] keys = node.keys;
		int i;

		// Find next node on path to appropriate leaf node
		for (i = 0; i < node.degree - 1; i++) {
			if (compareTo(key,keys[i])<0) { break; }
		}

		/* Return node if it is a LeafNode object,
		   otherwise repeat the search function a level down */
		Node childNode = node.childPointers[i];
		if (childNode instanceof LeafNode) {
			return (LeafNode)childNode;
		} else {
			return findLeafNode((InternalNode)node.childPointers[i], key);
		}
	}

	/**
	 * Given a list of pointers to Node objects, this method returns the index of
	 * the pointer that points to the specified 'node' LeafNode object.
	 * @param pointers: a list of pointers to Node objects
	 * @param node: a specific pointer to a LeafNode
	 * @return (int) index of pointer in list of pointers
	 */
	private int findIndexOfPointer(Node[] pointers, LeafNode node) {
		int i;
		for (i = 0; i < pointers.length; i++) {
			if (pointers[i] == node) { break; }
		}
		return i;
	}

	/**
	 * This is a simple method that returns the midpoint (or lower bound
	 * depending on the context of the method invocation) of the max degree m of
	 * the B+ tree.
	 * @return (int) midpoint/lower bound
	 */
	private int getMidpoint() {
		return (int)Math.ceil((this.m + 1) / 2.0) - 1;
	}

	/**
	 * Given a deficient InternalNode in, this method remedies the deficiency
	 * through borrowing and merging.
	 * @param in: a deficient InternalNode
	 */
	private void handleDeficiency(InternalNode in) {

		InternalNode sibling;
		InternalNode parent = in.parent;

		// Remedy deficient root node
		if (this.root == in) {
			for (int i = 0; i < in.childPointers.length; i++) {
				if (in.childPointers[i] != null) {
					if (in.childPointers[i] instanceof InternalNode) {
						this.root = (InternalNode)in.childPointers[i];
						this.root.parent = null;
					} else if (in.childPointers[i] instanceof LeafNode) {
						this.root = null;
					}
				}
			}
		}

		// Borrow:
		else if (in.leftSibling != null && in.leftSibling.isLendable()) {
			sibling = in.leftSibling;
		} else if (in.rightSibling != null && in.rightSibling.isLendable()) {
			sibling = in.rightSibling;

			// Copy 1 key and pointer from sibling (atm just 1 key)
			Object borrowedKey = sibling.keys[0];
			Node pointer = sibling.childPointers[0];

			// Copy root key and pointer into parent
			in.keys[in.degree - 1] = parent.keys[0];
			in.childPointers[in.degree] = pointer;

			// Copy borrowedKey into root
			parent.keys[0] = borrowedKey;

			// Delete key and pointer from sibling
			sibling.removePointer(0);
			Arrays.sort(sibling.keys);
			sibling.removePointer(0);
			shiftDown(in.childPointers, 1);
		}

		// Merge:
		else if (in.leftSibling != null && in.leftSibling.isMergeable()) {

		} else if (in.rightSibling != null && in.rightSibling.isMergeable()) {
			sibling = in.rightSibling;

			// Copy rightmost key in parent to beginning of sibling's keys &
			// delete key from parent
			sibling.keys[sibling.degree - 1] = parent.keys[parent.degree - 2];
			Arrays.sort(sibling.keys, 0, sibling.degree);
			parent.keys[parent.degree - 2] = null;

			// Copy in's child pointer over to sibling's list of child pointers
			for (int i = 0; i < in.childPointers.length; i++) {
				if (in.childPointers[i] != null) {
					sibling.prependChildPointer(in.childPointers[i]);
					in.childPointers[i].parent = sibling;
					in.removePointer(i);
				}
			}

			// Delete child pointer from grandparent to deficient node
			parent.removePointer(in);

			// Remove left sibling
			sibling.leftSibling = in.leftSibling;
		}

		// Handle deficiency a level up if it exists
		if (parent != null && parent.isDeficient()) {
			handleDeficiency(parent);
		}
	}

	/**
	 * This is a simple method that determines if the B+ tree is empty or not.
	 * @return a boolean indicating if the B+ tree is empty or not
	 */
	private boolean isEmpty() {
		return firstLeaf == null;
	}

	/**
	 * This method performs a standard linear search on a sorted
	 * DictionaryPair[] and returns the index of the first null entry found.
	 * Otherwise, this method returns a -1. This method is primarily used in
	 * place of binarySearch() when the target t = null.
	 * @param dps: list of dictionary pairs sorted by key within leaf node
	 * @return index of the target value if found, else -1
	 */
	private int linearNullSearch(DictionaryPair[] dps) {
		for (int i = 0; i <  dps.length; i++) {
			if (dps[i] == null) { return i; }
		}
		return -1;
	}

	/**
	 * This method performs a standard linear search on a list of Node[] pointers
	 * and returns the index of the first null entry found. Otherwise, this
	 * method returns a -1. This method is primarily used in place of
	 * binarySearch() when the target t = null.
	 * @param pointers: list of Node[] pointers
	 * @return index of the target value if found, else -1
	 */
	private int linearNullSearch(Node[] pointers) {
		for (int i = 0; i <  pointers.length; i++) {
			if (pointers[i] == null) { return i; }
		}
		return -1;
	}

	/**
	 * This method is used to shift down a set of pointers that are prepended
	 * by null values.
	 * @param pointers: the list of pointers that are to be shifted
	 * @param amount: the amount by which the pointers are to be shifted
	 */
	private void shiftDown(Node[] pointers, int amount) {
		Node[] newPointers = new Node[this.m + 1];
		for (int i = amount; i < pointers.length; i++) {
			newPointers[i - amount] = pointers[i];
		}
		pointers = newPointers;
	}

	/**
	 * This is a specialized sorting method used upon lists of DictionaryPairs
	 * that may contain interspersed null values.
	 * @param dictionary: a list of DictionaryPair objects
	 */
	private void sortDictionary(DictionaryPair[] dictionary) {
		Arrays.sort(dictionary, new Comparator<DictionaryPair>() {
			@Override
			public int compare(DictionaryPair o1, DictionaryPair o2) {
				if (o1 == null && o2 == null) { return 0; }
				if (o1 == null) { return 1; }
				if (o2 == null) { return -1; }
				return o1.compareTo(o2);
			}
		});
	}

	/**
	 * This method modifies the InternalNode 'in' by removing all pointers within
	 * the childPointers after the specified split. The method returns the removed
	 * pointers in a list of their own to be used when constructing a new
	 * InternalNode sibling.
	 * @param in: an InternalNode whose childPointers will be split
	 * @param split: the index at which the split in the childPointers begins
	 * @return a Node[] of the removed pointers
	 */
	private Node[] splitChildPointers(InternalNode in, int split) {

		Node[] pointers = in.childPointers;
		Node[] halfPointers = new Node[this.m + 1];

		// Copy half of the values into halfPointers while updating original keys
		for (int i = split + 1; i < pointers.length; i++) {
			halfPointers[i - split - 1] = pointers[i];
			in.removePointer(i);
		}

		return halfPointers;
	}

	/**
	 * This method splits a single dictionary into two dictionaries where all
	 * dictionaries are of equal length, but each of the resulting dictionaries
	 * holds half of the original dictionary's non-null values. This method is
	 * primarily used when splitting a node within the B+ tree. The dictionary of
	 * the specified LeafNode is modified in place. The method returns the
	 * remainder of the DictionaryPairs that are no longer within ln's dictionary.
	 * @param ln: list of DictionaryPairs to be split
	 * @param split: the index at which the split occurs
	 * @return DictionaryPair[] of the two split dictionaries
	 */
	private DictionaryPair[] splitDictionary(LeafNode ln, int split) {

		DictionaryPair[] dictionary = ln.dictionary;

		/* Initialize two dictionaries that each hold half of the original
		   dictionary values */
		DictionaryPair[] halfDict = new DictionaryPair[this.m];

		// Copy half of the values into halfDict
		for (int i = split; i < dictionary.length; i++) {
			halfDict[i - split] = dictionary[i];
			ln.delete(i);
		}

		return halfDict;
	}

	/**
	 * When an insertion into the B+ tree causes an overfull node, this method
	 * is called to remedy the issue, i.e. to split the overfull node. This method
	 * calls the sub-methods of splitKeys() and splitChildPointers() in order to
	 * split the overfull node.
	 * @param in: an overfull InternalNode that is to be split
	 */
	private void splitInternalNode(InternalNode in) {

		// Acquire parent
		InternalNode parent = in.parent;

		// Split keys and pointers in half
		int midpoint = getMidpoint();
		Object newParentKey = in.keys[midpoint];
		Object[] halfKeys = splitKeys(in.keys, midpoint);
		Node[] halfPointers = splitChildPointers(in, midpoint);

		// Change degree of original InternalNode in
		in.degree = linearNullSearch(in.childPointers);

		// Create new sibling internal node and add half of keys and pointers
		InternalNode sibling = new InternalNode(this.m, halfKeys, halfPointers);
		for (Node pointer : halfPointers) {
			if (pointer != null) { pointer.parent = sibling; }
		}

		// Make internal nodes siblings of one another
		sibling.rightSibling = in.rightSibling;
		if (sibling.rightSibling != null) {
			sibling.rightSibling.leftSibling = sibling;
		}
		in.rightSibling = sibling;
		sibling.leftSibling = in;

		if (parent == null) {

			// Create new root node and add midpoint key and pointers
			Object[] keys = new Object[this.m];
			keys[0] = newParentKey;
			InternalNode newRoot = new InternalNode(this.m, keys);
			newRoot.appendChildPointer(in);
			newRoot.appendChildPointer(sibling);
			this.root = newRoot;

			// Add pointers from children to parent
			in.parent = newRoot;
			sibling.parent = newRoot;

		} else {

			// Add key to parent
			parent.keys[parent.degree - 1] = newParentKey;
			Arrays.sort(parent.keys, 0, parent.degree);

			// Set up pointer to new sibling
			int pointerIndex = parent.findIndexOfPointer(in) + 1;
			parent.insertChildPointer(sibling, pointerIndex);
			sibling.parent = parent;
		}
	}

	/**
	 * This method modifies a list of Integer-typed objects that represent keys
	 * by removing half of the keys and returning them in a separate Integer[].
	 * This method is used when splitting an InternalNode object.
	 * @param keys: a list of Integer objects
	 * @param split: the index where the split is to occur
	 * @return Integer[] of removed keys
	 */
	private Object[] splitKeys(Object[] keys, int split) {

		Object[] halfKeys = new Object[this.m];

		// Remove split-indexed value from keys
		keys[split] = null;

		// Copy half of the values into halfKeys while updating original keys
		for (int i = split + 1; i < keys.length; i++) {
			halfKeys[i - split - 1] = keys[i];
			keys[i] = null;
		}

		return halfKeys;
	}

	/*~~~~~~~~~~~~~~~~ API: DELETE, INSERT, SEARCH ~~~~~~~~~~~~~~~~*/

	/**
	 * Given a key, this method will remove the dictionary pair with the
	 * corresponding key from the B+ tree.
	 * @param key: an integer key that corresponds with an existing dictionary
	 *             pair
	 */
	public void delete(Object key) {
		if (isEmpty()) {

			/* Flow of execution goes here when B+ tree has no dictionary pairs */

			System.err.println("Invalid Delete: The B+ tree is currently empty.");

		} else {

			// Get leaf node and attempt to find index of key to delete
			LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);
			int dpIndex = binarySearch(ln.dictionary, ln.numPairs, key);


			if (dpIndex < 0) {

				/* Flow of execution goes here when key is absent in B+ tree */

				System.err.println("Invalid Delete: Key unable to be found.");

			} else {

				// Successfully delete the dictionary pair
				ln.delete(dpIndex);

				// Check for deficiencies
				if (ln.isDeficient()) {

					LeafNode sibling;
					InternalNode parent = ln.parent;

					// Borrow: First, check the left sibling, then the right sibling
					if (ln.leftSibling != null &&
						ln.leftSibling.parent == ln.parent &&
						ln.leftSibling.isLendable()) {

						sibling = ln.leftSibling;
						DictionaryPair borrowedDP = sibling.dictionary[sibling.numPairs - 1];

						/* Insert borrowed dictionary pair, sort dictionary,
						   and delete dictionary pair from sibling */
						ln.insert(borrowedDP);
						sortDictionary(ln.dictionary);
						sibling.delete(sibling.numPairs - 1);

						// Update key in parent if necessary
						int pointerIndex = findIndexOfPointer(parent.childPointers, ln);
						if (!( compareTo(borrowedDP.key,parent.keys[pointerIndex - 1])>=0)) {
							parent.keys[pointerIndex - 1] = ln.dictionary[0].key;
						}

					} else if (ln.rightSibling != null &&
							   ln.rightSibling.parent == ln.parent &&
							   ln.rightSibling.isLendable()) {

						sibling = ln.rightSibling;
						DictionaryPair borrowedDP = sibling.dictionary[0];

						/* Insert borrowed dictionary pair, sort dictionary,
					       and delete dictionary pair from sibling */
						ln.insert(borrowedDP);
						sibling.delete(0);
						sortDictionary(sibling.dictionary);

						// Update key in parent if necessary
						int pointerIndex = findIndexOfPointer(parent.childPointers, ln);
						if (!(compareTo(borrowedDP.key,parent.keys[pointerIndex]) < 0 )) {
							parent.keys[pointerIndex] = sibling.dictionary[0].key;
						}

					}

					// Merge: First, check the left sibling, then the right sibling
					else if (ln.leftSibling != null &&
							 ln.leftSibling.parent == ln.parent &&
							 ln.leftSibling.isMergeable()) {

						sibling = ln.leftSibling;
						int pointerIndex = findIndexOfPointer(parent.childPointers, ln);

						// Remove key and child pointer from parent
						parent.removeKey(pointerIndex - 1);
						parent.removePointer(ln);

						// Update sibling pointer
						sibling.rightSibling = ln.rightSibling;

						// Check for deficiencies in parent
						if (parent.isDeficient()) {
							handleDeficiency(parent);
						}

					} else if (ln.rightSibling != null &&
							   ln.rightSibling.parent == ln.parent &&
							   ln.rightSibling.isMergeable()) {

						sibling = ln.rightSibling;
						int pointerIndex = findIndexOfPointer(parent.childPointers, ln);

						// Remove key and child pointer from parent
						parent.removeKey(pointerIndex);
						parent.removePointer(pointerIndex);

						// Update sibling pointer
						sibling.leftSibling = ln.leftSibling;
						if (sibling.leftSibling == null) {
							firstLeaf = sibling;
						}

						if (parent.isDeficient()) {
							handleDeficiency(parent);
						}
					}

				} else if (this.root == null && this.firstLeaf.numPairs == 0) {

					/* Flow of execution goes here when the deleted dictionary
					   pair was the only pair within the tree */

					// Set first leaf as null to indicate B+ tree is empty
					this.firstLeaf = null;

				} else {

					/* The dictionary of the LeafNode object may need to be
					   sorted after a successful delete */
					sortDictionary(ln.dictionary);

				}
			}
		}
	}

	/**
	 * Given an integer key and floating point value, this method inserts a
	 * dictionary pair accordingly into the B+ tree.
	 * @param key: an integer key to be used in the dictionary pair
	 * @param value: a floating point number to be used in the dictionary pair
	 */
	public void insert(Object key, String value){
		if(this.search(key)!=null) {
			Vector<String> locations= (Vector<String>) this.search(key);
			locations.add(value);
			this.searchDictionaryPair(key).value=locations;
			return;
		}
		if (isEmpty()) {

			/* Flow of execution goes here only when first insert takes place */

			// Create leaf node as first node in B plus tree (root is null)
			LeafNode ln = new LeafNode(this.m, new DictionaryPair(key, value));

			// Set as first leaf node (can be used later for in-order leaf traversal)
			this.firstLeaf = ln;

		} else {

			// Find leaf node to insert into
			LeafNode ln = (this.root == null) ? this.firstLeaf :
												findLeafNode(key);

			// Insert into leaf node fails if node becomes overfull
			if (!ln.insert(new DictionaryPair(key, value))) {

				// Sort all the dictionary pairs with the included pair to be inserted
				ln.dictionary[ln.numPairs] = new DictionaryPair(key, value);
				ln.numPairs++;
				sortDictionary(ln.dictionary);

				// Split the sorted pairs into two halves
				int midpoint = getMidpoint();
				DictionaryPair[] halfDict = splitDictionary(ln, midpoint);

				if (ln.parent == null) {

					/* Flow of execution goes here when there is 1 node in tree */

					// Create internal node to serve as parent, use dictionary midpoint key
					Object[] parent_keys = new Object[this.m];
					parent_keys[0] = halfDict[0].key;
					InternalNode parent = new InternalNode(this.m, parent_keys);
					ln.parent = parent;
					parent.appendChildPointer(ln);

				} else {

					/* Flow of execution goes here when parent exists */

					// Add new key to parent for proper indexing
					Object newParentKey = halfDict[0].key;
					ln.parent.keys[ln.parent.degree - 1] = newParentKey;
					Arrays.sort(ln.parent.keys, 0, ln.parent.degree);
				}

				// Create new LeafNode that holds the other half
				LeafNode newLeafNode = new LeafNode(this.m, halfDict, ln.parent);

				// Update child pointers of parent node
				int pointerIndex = ln.parent.findIndexOfPointer(ln) + 1;
				ln.parent.insertChildPointer(newLeafNode, pointerIndex);

				// Make leaf nodes siblings of one another
				newLeafNode.rightSibling = ln.rightSibling;
				if (newLeafNode.rightSibling != null) {
					newLeafNode.rightSibling.leftSibling = newLeafNode;
				}
				ln.rightSibling = newLeafNode;
				newLeafNode.leftSibling = ln;

				if (this.root == null) {

					// Set the root of B+ tree to be the parent
					this.root = ln.parent;

				} else {

					/* If parent is overfull, repeat the process up the tree,
			   		   until no deficiencies are found */
					InternalNode in = ln.parent;
					while (in != null) {
						if (in.isOverfull()) {
							splitInternalNode(in);
						} else {
							break;
						}
						in = in.parent;
					}
				}
			}
		}
	}

	/**
	 * Given a key, this method returns the value associated with the key
	 * within a dictionary pair that exists inside the B+ tree.
	 * @param key: the key to be searched within the B+ tree
	 * @return the floating point value associated with the key within the B+ tree
	 */
	public Vector<String> search(Object key) {

		// If B+ tree is completely empty, simply return null
		if (isEmpty()) { return null; }

		// Find leaf node that holds the dictionary key
		LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

		// Perform binary search to find index of key within dictionary
		DictionaryPair[] dps = ln.dictionary;
		int index = binarySearch(dps, ln.numPairs, key);

		// If index negative, the key doesn't exist in B+ tree
		if (index < 0) {
			return null;
		} else {
			return dps[index].value;
		}
	}
	
	public DictionaryPair searchDictionaryPair(Object key) {

		// If B+ tree is completely empty, simply return null
		if (isEmpty()) { return null; }

		// Find leaf node that holds the dictionary key
		LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

		// Perform binary search to find index of key within dictionary
		DictionaryPair[] dps = ln.dictionary;
		int index = binarySearch(dps, ln.numPairs, key);

		// If index negative, the key doesn't exist in B+ tree
		if (index < 0) {
			return null;
		} else {
			return dps[index];
		}
	}
	/**
	 * This method traverses the doubly linked list of the B+ tree and records
	 * all values whose associated keys are within the range specified by
	 * lowerBound and upperBound.
	 * @param lowerBound: (int) the lower bound of the range
	 * @param upperBound: (int) the upper bound of the range
	 * @return an ArrayList<Double> that holds all values of dictionary pairs
	 * whose keys are within the specified range
	 */
	public ArrayList<Object> search(Object lowerBound, Object upperBound) {

		// Instantiate Double array to hold values
		ArrayList<Object> values = new ArrayList<Object>();

		// Iterate through the doubly linked list of leaves
		LeafNode currNode = this.firstLeaf;
		while (currNode != null) {

			// Iterate through the dictionary of each node
			DictionaryPair dps[] = currNode.dictionary;
			for (DictionaryPair dp : dps) {

				/* Stop searching the dictionary once a null value is encountered
				   as this the indicates the end of non-null values */
				if (dp == null) { break; }

				// Include value if its key fits within the provided range
				if (compareTo(lowerBound,dp.key)<=0 && compareTo(dp.key,upperBound)<=0 ) {
					values.add(dp.value);
				}
			}

			/* Update the current node to be the right sibling,
			   leaf traversal is from left to right */
			currNode = currNode.rightSibling;

		}

		return values;
	}
	
	
	public static int compareTo(Object key1 , Object key2) {
		if (key1.getClass().getSimpleName().equals("String")) {
			String key3=((String)key1);
			String key4=((String)key2);
			if(key3.compareTo(key4)==0) {
				return 0;
			}else if(key3.compareTo(key4)>0) {
				return 1;
			}else{
				return -1;}
		}else if (key1.getClass().getSimpleName().equals("Integer")){
			int key3=(int)key1;
			int key4=(int)key2;
			if(key3==key4) {
				return 0;
			}else if(key3>key4) {
				return 1;
			}else{
				return -1;}
			
		}else {
			double key3=(double)key1;
			double key4=(double)key2;
			if(key3==key4) {
				return 0;
			}else if(key3>key4) {
				return 1;
			}else{
				return -1;}
		}
	}

	/**
	 * Constructor
	 * @param m: the order (fanout) of the B+ tree
	 */
	public bplustree(int m) {
		this.m = m+1;
		this.root = null;
	}
}

