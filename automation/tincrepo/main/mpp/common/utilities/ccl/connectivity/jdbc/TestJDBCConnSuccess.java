import java.sql.Connection;

public class TestJDBCConnSuccess
{
    public static void main(String[] argv){
        try {
            ConnectionManager cm = new ConnectionManager();
            Connection conn = cm.getConnection();
            if (conn != null) {
                System.out.println("connect success :" + cm.getUrl());
            } else {
                System.out.println("connect fail");
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}
