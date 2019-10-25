
public class TestJDBCInsertSQL
{
    public static void main(String[] argv){
        try {
            JDBCCommon jdbc = new JDBCCommon();
            if (!jdbc.createTable("jdbctable", "CREATE TABLE jdbctable (a int)")) {
                System.exit(1);
            }
            
            if (!jdbc.insertData("INSERT INTO jdbctable VALUES (1)")) {
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
