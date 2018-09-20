import java.sql.Connection;

public class TestJDBCConnFail
{
    public static void main(String[] argv){
        try {
            ConnectionManager cm = new ConnectionManager();
            cm.setUrl(cm.getHostPort(), "no_exist_db");
            Connection conn = cm.getConnection();
            if (conn != null) {
                System.out.println("connect unexpect success :" + cm.getUrl());
                System.exit(1);
            } else {
                System.out.println("connect expect fail : " + cm.getUrl());
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        System.exit(0);
    }
}
