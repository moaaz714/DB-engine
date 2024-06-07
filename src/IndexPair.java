import java.io.Serializable;

public class IndexPair implements Serializable{
	String indexName;
	bplustree bplustree;
	
	public IndexPair(String indexName,bplustree bplustree) {
		this.indexName=indexName;
		this.bplustree=bplustree;
	}
}
