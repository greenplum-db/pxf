import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionManager
{
    private String username = System.getProperty("user.name");
    private String database = "template1";
    private String hostname = "127.0.0.1";
    private String password = "";
    private String port = "5432";
    private String connurl = "";

    public ConnectionManager() {
        this.setUsername(System.getenv("PGUSER"));
        this.setDatabase(System.getenv("PGDATABASE"));
        this.setHostname(System.getenv("PGHOST"));
        this.setPort(System.getenv("PGPORT"));
        this.setUrl(this.getHostPort(), this.database);
    }

    public void setUsername(String username) {
        if (username != null) {
            this.username = username;
        }
    }

    public void setDatabase(String database) {
        if (database != null) {
            this.database = database;
        }
    }

    public void setHostname(String hostname) {
        if (hostname != null) {
            this.hostname = hostname;
        }
    }

    public void setPassword(String password) {
        if (password != null) {
            this.password = password;
        }
    }

    public void setPort(String port) {
        if (port != null) {
            this.port = port;
        }
    }    

    public void setUrl(String hostAndPort, String database) {
        this.connurl = "jdbc:postgresql://" + hostAndPort + "/" + database;
    }

    public String getHostPort() {
        return this.hostname + ":" + this.port;
    }

    public String getUrl() {
        return this.connurl;
    } 

    public String getUsername() {
        return this.username;
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.connurl, this.username, "");
        } catch (SQLException se) {
            System.out.println("Connection String : " + this.connurl);
            System.out.println("username = " + this.username);
            System.out.println("Couldn't connect: print out a stack trace and exit.");
            se.printStackTrace();
        }
        return conn;
    }
}
