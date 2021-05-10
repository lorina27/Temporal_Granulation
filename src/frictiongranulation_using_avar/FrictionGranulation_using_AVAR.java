
package frictiongranulation_using_avar;

import java.sql.*;

/**
 *
 * @author lorina
 */
public class FrictionGranulation_using_AVAR {

    private final String url = "jdbc:postgresql://10.91.224.230:5432/nsf_roadtraffic_friction_v2";
    private final String user = "lsinanaj";
    private final String password = "02Apr2020";

    public Connection connect() {
        Connection con = null;
        try {
            con = DriverManager.getConnection(url, user, password);

            // Create a statement for SQL query - move the cursor either forward or backward
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            //record the execution time
            long executionTime = 0;
            long start = System.currentTimeMillis();

            // Insert results to the Granularized Frictions Table.
            String query3 = " insert into granulated_frictions_table (unix_time_startinterval, average_friction)"
                    + " values (?,?)";
            PreparedStatement ps = con.prepareStatement(query3);

            //AVAR value=16 milliseconds, this query finds the average fl_friction_noisy every 16 millis
            String query1 = "SELECT \n"
                    + "     floor(unix_time / EXTRACT(epoch FROM interval '0.016 seconds'))\n"
                    + "     * EXTRACT(epoch FROM interval ' 0.016 seconds') AS window\n"
                    + "     , avg(fl_friction_noisy) as fr\n"
                    + " FROM friction_measurement_uml_avar\n"
                    + " group by 1\n"
                    + " ORDER BY 1 ASC;";
            ResultSet rs1 = stmt.executeQuery(query1);

            System.out.println("results queried...");

            while (rs1.next()) {
                ps.setDouble(1, rs1.getDouble("window"));
                ps.setDouble(2, rs1.getDouble("fr"));

                ps.addBatch();
            }

            System.out.println("executing batch...");

            ps.executeBatch();

            System.out.println("batch executed");
            ps.close();

            // Record the time the query stops running
            long end = System.currentTimeMillis();
            // The difference in time from start to end of running
            executionTime = end - start;

            System.out.println("Execution time: " + executionTime + " milliseconds");

            // Close the connection object  
            con.close();

            //System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return con;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        FrictionGranulation_using_AVAR app = new FrictionGranulation_using_AVAR();
        app.connect();

    }

}
