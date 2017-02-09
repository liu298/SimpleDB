import java.sql.*;
import simpledb.remote.*;
import java.io.*;
import java.util.*;
import simpledb.parse.*;
import simpledb.query.*;
import simpledb.server.*;
import simpledb.metadata.*;
import simpledb.tx.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class VisualizationSimpleDB {
    private static Connection conn = null;

    public static void main(String[] args) {
	   try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			Reader rdr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(rdr);

			while (true) {
				// process one line of input
				System.out.print("\nSQL> ");
				String cmd = br.readLine().trim();
				System.out.println();
				if (cmd.startsWith("exit"))
					break;
				else if (cmd.startsWith("select"))
					doStep(cmd);
		    }
	    }
	    catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

    private static void doQuery(String cmd) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            ResultSetMetaData md = rs.getMetaData();
            int numcols = md.getColumnCount();
            int totalwidth = 0;

            // print header
            for(int i=1; i<=numcols; i++) {
                int width = md.getColumnDisplaySize(i);
                totalwidth += width;
                String fmt = "%" + width + "s";
                System.out.format(fmt, md.getColumnName(i));
            }
            System.out.println();
            for(int i=0; i<totalwidth; i++)
                System.out.print("-");
            System.out.println();

            // print records
            while(rs.next()) {
                for (int i=1; i<=numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int fldtype = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fldtype == Types.INTEGER)
                        System.out.format(fmt + "d", rs.getInt(fldname));
                    else
                        System.out.format(fmt + "s", rs.getString(fldname));
                }
                System.out.println();
            }
            rs.close();
        }
        catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void doStep(String cmd) {
        Transaction tx = new Transaction();
//        Collection<String> qrys = new ArrayList<String>();
        Parser parser = new Parser(cmd);
        QueryData data = parser.query();
        Collection<String> fields = data.fields();
        Collection<String> tables = data.tables();
        Predicate pred = data.pred();

        // print query tree summary
        int nodeCount = 1;
        System.out.println("The Query Tree:");
        for (String tableName:tables)
            System.out.println("Node " + nodeCount++ +": TablePlan on table " + tableName);
        if (tables.size() > 1){
            // create array of previous node
            List<String> array = new ArrayList<String>();
            for (int i = 1; i <= nodeCount; i++)
                array.add(Integer.toString(i));
            String joinNodeNum = String.join(",",array);
            System.out.println("Node " + nodeCount++ +": ProductPlan from Node " + joinNodeNum);
        }

        if (pred != null)
            System.out.format("Node " + nodeCount++ +": SelectPlan from Node %d", nodeCount-2);

        System.out.format("Node " + nodeCount +": ProjectPlan from Node %d", nodeCount-1);
        System.out.println();

        // print every node
        nodeCount = 1;

        // TablePlan
        for (String tableName: tables){
            System.out.println("Node " + nodeCount++ +" Outputs:");
            Collection<String> tableFields = SimpleDB.mdMgr().getTableInfo(tableName,tx).schema().fields();
            Collection<String> tempTables = new ArrayList<String>();
            tempTables.add(tableName);
            Predicate tempPred = new Predicate();
            QueryData temp = new QueryData(tableFields,tempTables,tempPred);
            String tempCmd = temp.toString();
            doQuery(tempCmd);
        }

        // ProductPlan
        if (tables.size() > 1) {
            System.out.println("Node " + nodeCount++ + " Outputs:");
            Collection<String> tempFields = new ArrayList<>();
            for (String tablename : tables)
                tempFields.addAll(SimpleDB.mdMgr().getTableInfo(tablename, tx).schema().fields());
            Predicate tempPred = new Predicate();
            QueryData temp = new QueryData(tempFields, tables, tempPred);
            String tempCmd = temp.toString();
            doQuery(tempCmd);
        }

        // SelectPlan
        if (pred != null) {
            System.out.println("Node " + nodeCount++ + " Outputs:");
            Collection<String> tempFields = new ArrayList<>();
            for (String tablename : tables)
                tempFields.addAll(SimpleDB.mdMgr().getTableInfo(tablename, tx).schema().fields());
            QueryData temp = new QueryData(tempFields, tables, pred);
            String tempCmd = temp.toString();
            doQuery(tempCmd);
        }

        // ProjectPlan
        System.out.println("Node " + nodeCount + " Outputs:");
        QueryData temp = new QueryData(fields, tables, pred);
        String tempCmd = temp.toString();
        doQuery(tempCmd);
    }

}