import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;

public class JDBCCommon
{
    public boolean createTable(String tblname, String sql) throws SQLException {
        if (!this.dropTable(tblname)) {
            return false;
        }

        ConnectionManager cm = null;
        Connection conn = null;
        Statement stmt = null;
        try {
            cm = new ConnectionManager();
            conn = cm.getConnection();
            if (conn == null) {
                System.out.println("connect fail : " + cm.getUrl());
                return false;
            }
            System.out.println("connect success : " + cm.getUrl());

            stmt = conn.createStatement();
            if (null == stmt) {
                System.out.println("connection create statement fail");
                return false; 
            }
            System.out.println("connection create statement success");

            stmt.execute(sql);
            System.out.println("statement execute success : " + sql);

        } catch (SQLException e) {
            e.printStackTrace();
            return false;

        } finally {
            stmt.close();
            conn.close();
        
        }
        return true;
    }

    public boolean dropTable(String tblname) throws SQLException {
        ConnectionManager cm = null;
        Connection conn = null;
        Statement stmt = null;
        try {
            cm = new ConnectionManager();
            conn = cm.getConnection();
            if (conn == null) {
                System.out.println("connect fail : " + cm.getUrl());
                return false;
            }
            System.out.println("connect success : " + cm.getUrl());

            stmt = conn.createStatement();
            if (null == stmt) {
                System.out.println("connection create statement fail");
                return false; 
            }
            System.out.println("connection create statement success");

            String sql = "DROP TABLE IF EXISTS " + tblname;
            stmt.execute(sql);
            System.out.println("statement execute success : " + sql);

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        
        } finally {
            stmt.close();
            conn.close();
        } 
        return true;
   
    }

    public boolean insertData(String sql) throws SQLException {
        ConnectionManager cm = null;
        Connection conn = null;
        Statement stmt = null;
        try {
            cm = new ConnectionManager();
            conn = cm.getConnection();
            if (conn == null) {
                System.out.println("connect fail : " + cm.getUrl());
                return false;
            }
            System.out.println("connect success : " + cm.getUrl());

            stmt = conn.createStatement();
            if (null == stmt) {
                System.out.println("connection create statement fail");
                return false; 
            }
            System.out.println("connection create statement success");

            int num = stmt.executeUpdate(sql);
            if (num != 1) {
                System.out.println("statement execute fail : " + sql);
                return false;
            }
            System.out.println("statement execute success : " + sql);

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        
        } finally {
            stmt.close();
            conn.close();
        }
        return true;
    }

}
