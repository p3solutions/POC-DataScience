package optimised_trie;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
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
    HashSet<TrieNode> childList; 
 
    public TrieNode(char c)
    {
        childList = new HashSet<TrieNode>();
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
        List<String> toFind = new ArrayList<>(); 
        final List<String> tableList = new ArrayList<>();
        HashMap<String, Boolean> results = new HashMap<>();
        int count, temp;
        Instant start = Instant.now();

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
            
            String  col1, col2, sql1, sql2;
            while (rst.next()) 
              tableList.add(rst.getString(3));
//            System.out.println("TABLE LIST:"+tableList);
            System.out.println("Got Table List - "+new Timestamp(System.currentTimeMillis()));

            for(int i = 0; i<tableList.size(); i++) {
                System.out.println("Primary table "+tableList.get(i));
                ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM "+tableList.get(i));

//              ResultSetMetaData rsmd1 = rs.getMetaData();
                for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
                    col1 = rs.getMetaData().getColumnName(j+1);
                    System.out.println("Before getting the table list - "+new Timestamp(System.currentTimeMillis()));

                    sql1 = "select `"+col1+"` from "+tableList.get(i);
                    System.out.println("After getting the table list - "+new Timestamp(System.currentTimeMillis()));

                    System.out.println("SQL1 "+sql1);
                    System.out.println("Primary Column "+col1);
                    ResultSet rs1 = conn.createStatement().executeQuery(sql1);
                    temp = 0;
                    System.out.println("Before creating trie - "+new Timestamp(System.currentTimeMillis()));

                    Trie trie = new Trie();
                    while(rs1.next()) {
                        temp++;
                        if(rs1.getString(col1) == null)
                            continue;
                        trie.insert(rs1.getString(col1));
                    }
                    System.out.println("After creating trie - "+new Timestamp(System.currentTimeMillis()));
                                    
                    for (int k = i+1; k < tableList.size(); k++) {
                        System.out.println("Secondary table "+tableList.get(k));

                        ResultSet rss = conn.createStatement().executeQuery("SELECT * FROM "+tableList.get(k));
                        System.out.println("Received - "+new Timestamp(System.currentTimeMillis()));

                        for (int l = 0; l < rss.getMetaData().getColumnCount(); l++) {
                            col2 = rss.getMetaData().getColumnName(l+1);

                            sql2 = "select `"+col2+"` from "+tableList.get(k);
//                          System.out.println("Secondary Column "+col2);
                            System.out.println("SQL2  "+sql2);
                            ResultSet rs2 = conn.createStatement().executeQuery(sql2);
                            toFind = new ArrayList<>();
                            while(rs2.next())
                            {
                                String param2 = rs2.getString(col2);
                                if(param2 == null)
                                    continue;
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
                            if(temp!=0)
                                System.out.println("MATCH = "+count*100/temp+"%");
                            toFind = new ArrayList<>();

                        }
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

    }
}
