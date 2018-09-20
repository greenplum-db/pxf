import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

public class TestJDBCTransactionRollback
{
    public static void main(String[] argv){
        try {
            JDBCCommon jdbc = new JDBCCommon();
            if (!jdbc.createTable("jdbctable", "CREATE TABLE jdbctable (a int)")) {
                System.exit(1);
            }
           
            try {
                ConnectionManager cm = new ConnectionManager();
                Connection conn = cm.getConnection();
                if (conn == null) {
                    System.out.println("connect fail");
                    System.exit(1);
                }
                System.out.println("connect success :" + cm.getUrl());

                int a1 = 1;
                int a2 = 2;

                conn.setAutoCommit(false);
                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO jdbctable VALUES (?)");
                pstmt.setInt(1, a1);
                pstmt.executeUpdate();

                pstmt.setInt(1, a2);
                pstmt.executeUpdate();
               
                conn.rollback();
                conn.setAutoCommit(true);

                Statement stmt = conn.createStatement();
                if (null == stmt) {
                    System.out.println("connection create statement fail");
                    System.exit(1);
                }
                System.out.println("connection create statement success");

                String sql = "SELECT a FROM jdbctable ORDER BY a";
                ResultSet rs = stmt.executeQuery(sql);
                System.out.println("statement execute success : " + sql);
            
                int num = 0;
                while (rs.next()) {
                    System.out.println(rs.getInt("a"));
                    num++;
                }
                if (num != 0) {
                    System.out.println("SELECT ERROR : " + sql);
                    System.exit(1);
                }

                rs.close();
                stmt.close();
                pstmt.close();
                conn.close();

            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
       
            if (!jdbc.dropTable("jdbctable")) {
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        System.exit(0);
    }
}
