package optimised_trie;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




class TrieNode 
{
    char content; 
    boolean isEnd; 
    int count;  
    LinkedList<TrieNode> childList; 
 
    public TrieNode(char c)
    {
        childList = new LinkedList<TrieNode>();
        isEnd = false;
        content = c;
        count = 0;
    }  
    public TrieNode subNode(char c)
    {
        if (childList != null)
            for (TrieNode eachChild : childList)
                if (eachChild.content == c)
                    return eachChild;
        return null;
    }
}
 
class Trie
{
    private TrieNode root;
 
    public Trie()
    {
        root = new TrieNode(' '); 
    }
    public void insert(String word)
    {
        if (search(word) == true) 
            return;        
        TrieNode current = root; 
        for (char ch : word.toCharArray() )
        {
            TrieNode child = current.subNode(ch);
            if (child != null)
                current = child;
            else 
            {
                 current.childList.add(new TrieNode(ch));
                 current = current.subNode(ch);
            }
            current.count++;
        }
        current.isEnd = true;
    }
    public boolean search(String word)
    {
        TrieNode current = root;  
        for (char ch : word.toCharArray() )
        {
            if (current.subNode(ch) == null)
                return false;
            else
                current = current.subNode(ch);
        }      
        if (current.isEnd == true) 
            return true;
        return false;
    }
}    

public class Optimised {
	public static void main(String[] args) {
		String url = "jdbc:mysql://localhost:3306/sakila";
        String user = "root";
        String password = "secret";
        List<String> setOfStrings = new ArrayList<>();
        List<String> toFind = new ArrayList<>(); 
        final List<String> tableList = new ArrayList<>();
        HashMap<String, Boolean> results = new HashMap<>();
        int count, v, temp, counter;
        Runtime r = Runtime.getRuntime();
        
        
        try {
        	Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rst = md.getTables(null, null, "%", null);
            String  col1, col2, sql1, sql2;
            while (rst.next()) 
              tableList.add(rst.getString(3));
//            System.out.println("TABLE LIST:"+tableList);
            for(int i = 0; i<tableList.size(); i++) {
			    Statement stmt = conn.createStatement();
			    System.out.println("Primary table "+tableList.get(i));
			    ResultSet rs = stmt.executeQuery("SELECT * FROM "+tableList.get(i));
			    ResultSetMetaData rsmd1 = rs.getMetaData();
			    for (int j = 0; j < rsmd1.getColumnCount(); j++) {
			    	col1 = rsmd1.getColumnName(j+1);
		            Statement stmt1 = conn.createStatement();
		            sql1 = "select `"+col1+"` from "+tableList.get(i);
		            if(col1.equals("address2") || col1.equals("original_language_id") || col1.equals("SUB_ID") || col1.equals("rental_id") || col1.equals("return_date") || col1.equals("picture") || col1.equals("password"))
		            	continue;
		            System.out.println("SQL1 "+sql1);
		            System.out.println("Primary Column "+col1);
		            ResultSet rs1 = stmt1.executeQuery(sql1);
		            setOfStrings = new ArrayList<>();
		            while(rs1.next())
		            {
		            	String param1 = rs1.getString(col1);
		            	setOfStrings.add(param1);
		            	}
	            	temp = setOfStrings.size();
//	            	System.out.println("Set of strings: "+setOfStrings);
	            	r.gc();
	            	Trie trie = new Trie();
			        for(String a : setOfStrings) 
			        	trie.insert(a);
//			        System.out.println("Trie constructed");
			        
				    for (int k = i+1; k < tableList.size(); k++) {
					    Statement stmtt = conn.createStatement();
					    System.out.println("Secondary table "+tableList.get(k));

					    ResultSet rss = stmtt.executeQuery("SELECT * FROM "+tableList.get(k));
					    ResultSetMetaData rsmd2 = rss.getMetaData();
					    for (int l = 0; l < rsmd2.getColumnCount(); l++) {
					    	col2 = rsmd2.getColumnName(l+1);

				            Statement stmt2 = conn.createStatement();
				            sql2 = "select `"+col2+"` from "+tableList.get(k);
//				            System.out.println("Secondary Column "+col2);
				            System.out.println("SQL2  "+sql2);
				            ResultSet rs2 = stmt2.executeQuery(sql2);
				            toFind = new ArrayList<>();
				            while(rs2.next())
				            {
				            	String param2 = rs2.getString(col2);
				            	toFind.add(param2+"");
				            	}
				            results = new HashMap<>();
				            for(String s : toFind) {
					        	results.put(s, trie.search(s));
					        }
				            count = 0;
					        for(Map.Entry entry:results.entrySet()){
					        	if((Boolean)entry.getValue()==true) {
					        		count++;
					        	}
					        }
					        System.out.println("MATCH = "+count*100/temp+"%");
					       
					    }
				    }
			    }
            }
        }catch(Exception e) {
        	e.printStackTrace();
        }
	}
}

