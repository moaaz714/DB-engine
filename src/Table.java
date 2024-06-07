import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable {
	String tableName;
	Page[] pageList;
	String metaData;
	IndexPair[] indexPairs;
	int ColNum = 0;

	public Table(String tableName, int ColNum) {
		this.tableName = tableName;
		this.ColNum = ColNum;
		this.pageList = new Page[0];
	}

	public String getTableName() {
		return this.tableName;
	}
	
	public String toString() {
		
		String out="";
		for(int i=0;i<pageList.length;i++) {
			int j=i+1;
			Page page=pageList[i];
			out+="Page "+j+": "+page.toString()+"\n";
		}
		return out;	
	}
	
	public Page createpage(int maxEntries) {
		Page page = new Page(tableName + (pageList.length + 1), maxEntries);
		addPage(page);
		return page;
	}

	public void addPage(Page page) {
		int size = pageList.length;

		if (size == 0) {
			pageList = new Page[1];
			pageList[0] = page;
			return;
		}

		Page[] temp = new Page[size + 1];
		String strTableName = page.getPageName();

		int i;

		for (i = size - 1; (i >= 0 && pageList[i].getPageName().compareTo(
				strTableName) >= 0); i--) {
			if (pageList[i].getPageName().compareTo(strTableName) > 0) {
				temp[i + 1] = pageList[i];
			}
		}

		temp[i + 1] = page;

		for (int j = i; j >= 0; j--) {
			temp[j] = pageList[j];
		}
		pageList = temp;

	}

	public void addEntryAtPosition(int startingPage, int shiftPage,
			Tuple tuple, int position,int primaryKeyPosition) throws DBAppException {

		// startingPage and shiftPage is the position of the page within the
		// pageList
		
		
		Vector<Tuple> vector = this.pageList[startingPage].deserializeAll();
		
		if(position>vector.capacity()-1) {
			throw new DBAppException("You are trying to exceed page capacity.");
		}
		
		if (position > vector.size()) {
			throw new DBAppException("You are trying to skip a valid space to place the entry within the vector.");
		}
		
		this.shiftingHelper(startingPage, shiftPage, position, tuple,primaryKeyPosition);

	}

	private void shiftingHelper(int startingPage, int shiftPage, int position,
			Tuple tuple,int primaryKeyPosition) {

		// i want to shift the values down starting from index startingPage, The
		// shifting AT starting page will be
		// handled using the .add method
		// shiftPage will always initially be the index of the lastPage
		// the helper uses shiftPage as an indicator of which page i am shifting
		// a value
		// to rn
		if (startingPage == shiftPage) {
			// i reached the base case, this may be the last page (which can be
			// full or not)
			// if it is a middle page, an empty spot should already be created
			// by the
			// recurrsion
			Page page = pageList[shiftPage];
			Vector<Tuple> shiftPageVector = page.deserializeAll();
			if (shiftPageVector.size() == shiftPageVector.capacity()) {
				// page that i am trying to shift to is full
				// create a new page
				this.createpage(DBApp.maxEntriesPerPage);
				Vector<Tuple> lastPageVector = pageList[pageList.length - 1]
						.deserializeAll();
				lastPageVector.add((Tuple) shiftPageVector.lastElement());
				shiftPageVector.remove(shiftPageVector.get(shiftPageVector
						.size() - 1));
				
				shiftPageVector.add(position, tuple);
				pageList[shiftPage].serialize(shiftPageVector);
				pageList[pageList.length - 1].serialize(lastPageVector);

				pageList[shiftPage].minClustKey = shiftPageVector.get(0).values.get(primaryKeyPosition);
				pageList[pageList.length - 1].minClustKey = lastPageVector.get(0).values.get(primaryKeyPosition);

				pageList[shiftPage].maxClustKey = shiftPageVector.get(shiftPageVector.size() - 1).values.get(primaryKeyPosition);
				pageList[pageList.length - 1].maxClustKey = lastPageVector.get(lastPageVector.size() - 1).values.get(primaryKeyPosition);

			} else {
				shiftPageVector.add(position, tuple);

				pageList[shiftPage].minClustKey = shiftPageVector.get(0).values.get(primaryKeyPosition);
				pageList[shiftPage].maxClustKey = shiftPageVector.get(shiftPageVector.size() - 1).values.get(primaryKeyPosition);

				pageList[shiftPage].serialize(shiftPageVector);

			}
		} else {
			// startingpage is not the same as shiftPage , i need to move one
			// entry from the
			// page before shift page into
			// shiftpage and then redo the same for the previous page
			Page page = pageList[shiftPage];
			Vector<Tuple> shiftPageVector = page.deserializeAll();
			Vector<Tuple> pageBeforeShiftVector = pageList[shiftPage - 1].deserializeAll();

			if (shiftPageVector.size() == shiftPageVector.capacity()) {
				// page that i am trying to shift to is full
				// last page
				// create a new page to move an entry into then perform the
				// original needed
				// shifting
				this.createpage(DBApp.maxEntriesPerPage);
				Vector<Tuple> lastPageVector = pageList[pageList.length - 1]
						.deserializeAll();
				lastPageVector.add((Tuple) shiftPageVector.lastElement());
				shiftPageVector.remove(shiftPageVector.get(shiftPageVector.size() - 1));

				shiftPageVector.add(0,(Tuple) pageBeforeShiftVector.lastElement());
				pageBeforeShiftVector.remove(pageBeforeShiftVector.size() - 1);

				pageList[shiftPage].serialize(shiftPageVector);
				pageList[pageList.length - 1].serialize(lastPageVector);
				pageList[shiftPage - 1].serialize(pageBeforeShiftVector);

				pageList[shiftPage].minClustKey = shiftPageVector.get(0).values.get(primaryKeyPosition);
				pageList[shiftPage - 1].minClustKey = pageBeforeShiftVector.get(0).values.get(primaryKeyPosition);
				pageList[pageList.length - 1].minClustKey = lastPageVector.get(0).values.get(primaryKeyPosition);

				pageList[shiftPage].maxClustKey = shiftPageVector.get(shiftPageVector.size() - 1).values.get(primaryKeyPosition);
				pageList[shiftPage - 1].maxClustKey = pageBeforeShiftVector.get(pageBeforeShiftVector.size() - 1).values.get(primaryKeyPosition);
				pageList[pageList.length - 1].maxClustKey = lastPageVector.get(lastPageVector.size() - 1).values.get(primaryKeyPosition);

				shiftingHelper(startingPage, shiftPage - 1, position, tuple,primaryKeyPosition);

			} else {

				shiftPageVector.add(0,(Tuple) pageBeforeShiftVector.lastElement());
				pageBeforeShiftVector.remove(pageBeforeShiftVector.size() - 1);
				pageList[shiftPage].serialize(shiftPageVector);
				pageList[shiftPage - 1].serialize(pageBeforeShiftVector);

				pageList[shiftPage].minClustKey = shiftPageVector.get(0).values.get(primaryKeyPosition);
				pageList[shiftPage - 1].minClustKey = pageBeforeShiftVector.get(0).values.get(primaryKeyPosition);

				pageList[shiftPage].maxClustKey = shiftPageVector.get(shiftPageVector.size() - 1).values.get(primaryKeyPosition);
				pageList[shiftPage - 1].maxClustKey = pageBeforeShiftVector.get(pageBeforeShiftVector.size() - 1).values.get(primaryKeyPosition);

				shiftingHelper(startingPage, shiftPage - 1, position, tuple,primaryKeyPosition);
			}

		}

	}

	public int pageChoiceHelper(Object value, int low, int high) {

		// returns the position of the page in which to perform binary search on
		// in other words , it returns the position of the page which will be
		// the startPage
		// variable of the addEntryAtPosition(int startingPage,int shiftPage,
		// Tuple tuple, int position) method
		//value is the clusterring key value that im searching for a postion 
		
		
		if (high == low) {
			
			Page page=pageList[high];

			
			if(page.maxClustKey==null) {
				//empty page (final Page)
				return high;
				
			}
			
			
			if (value instanceof Integer) {

				
				if ( (int) value > (int) page.maxClustKey) {
					return high+1;
				} 
				else {
					
					return high;
				}
			}
			if (value instanceof String) {

				page=pageList[high];
				String strVal=(String) value;
				String strMax=(String) page.maxClustKey;
				if ( strVal.compareTo(strMax)>0) {
					return high+1;
				} 
				else {
					
					return high;
				}
			}
			
			if (value instanceof Double) {

				page=pageList[high];
				
				if ( (Double) value > (Double) page.maxClustKey) {
					return high+1;
				} 
				else {
					
					return high;
				}
			}
			
			
		}
			

		if (value instanceof Integer) {

			int mid = low + ((high - low) / 2);
			Page page = pageList[mid];
			if ((int) page.minClustKey <= (int) value
					&& (int) value <= (int) page.maxClustKey) {
				return mid;
			}

			else if ((int) page.minClustKey > (int) value) {

				return pageChoiceHelper(value, low, mid - 1);
			} else {

				return pageChoiceHelper(value, mid + 1, high);

			}
		} else if (value instanceof String) {

			int mid = low + ((high - low) / 2);
			Page page = pageList[mid];
			String minClustString = (String) page.minClustKey;
			String valueString = (String) value;
			String maxClustString = (String) page.maxClustKey;
			if (minClustString.compareTo(valueString) <= 0
					&& (maxClustString).compareTo(valueString) >= 0) {
				return mid;
			}

			else if (minClustString.compareTo(valueString) > 0) {

				return pageChoiceHelper(value, low, mid - 1);
			} else {

				return pageChoiceHelper(value, mid + 1, high);

			}

		} else {

			int mid = low + ((high - low) / 2);
			Page page = pageList[mid];
			if ((Double) page.minClustKey <= (Double) value
					&& (Double) value <= (Double) page.maxClustKey) {
				return mid;
			}

			else if ((Double) page.minClustKey > (Double) value) {

				return pageChoiceHelper(value, low, mid - 1);
			} else {

				return pageChoiceHelper(value, mid + 1, high);

			}

		}

	}

	// The method findPage searched in pageList using binary search

	public Page findPage(int low, int high, String strPageName)
			throws DBAppException { // low =0 high = listlength-1
		if (high < low) {
			throw new DBAppException("No table found with this name");
		}
		int mid = (low + high) / 2;

		if (pageList[mid].getPageName().compareTo(strPageName) == 0)
			return pageList[mid];
		if (pageList[mid].getPageName().compareTo(strPageName) < 0)
			return findPage((mid + 1), high, strPageName);

		return findPage(low, (mid - 1), strPageName);
	}

	public int getColNum() {
		return this.ColNum;
	}
	
	public void removePage(Page page){
		int size =pageList.length;
		
		if(size==0) {
			this.pageList= new Page[0];
			return;
		}
		
		Page[]temp= new Page[size-1]; 
		String strTableName = page.getPageName();
		
		int i;
		
		
		
		for(i=size-1;(i>=1 && pageList[i].getPageName().compareTo(strTableName)!=0);i--) {
			temp[i-1]=pageList[i];
		}
		
		
		for(int j=i-1;j>=0;j--) {
			temp[j]=pageList[j];
		}
		
		pageList=temp;
		
	}

	public void addIndexPair(IndexPair indexPair) {
				int size = indexPairs.length;
				if (size == 0) {
					indexPairs = new IndexPair[1];
					indexPairs[0] = indexPair;
					return;
				}

				IndexPair[] temp = new IndexPair[size + 1];
				String strIndex = indexPair.indexName;

				int i;

				for (i = size - 1; (i >= 0 && indexPairs[i].indexName.compareTo(strIndex) >= 0); i--) {
					if (indexPairs[i].indexName.compareTo(strIndex) == 0) {
						System.out.println("There is table already created with the same name");
					} else if (indexPairs[i].indexName.compareTo(strIndex) > 0) {
						temp[i + 1] = indexPairs[i];
					}
				}

				temp[i + 1] = indexPair;

				for (int j = i; j >= 0; j--) {
					temp[j] = indexPairs[j];
				}
				indexPairs = temp;

			}

	public IndexPair findIndexPair(int low, int high, String strIndex)
					throws DBAppException {
				if (high < low) {
					throw new DBAppException("No table found with this name");
				}
				int mid = (low + high) / 2;

				if (indexPairs[mid].indexName.compareTo(strIndex) == 0)
					return indexPairs[mid];
				if (indexPairs[mid].indexName.compareTo(strIndex) < 0)
					return findIndexPair((mid + 1), high, strIndex);

				return findIndexPair(low, (mid - 1), strIndex);
			}

}