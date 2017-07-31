/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package migracions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
/**
 *
 * @author Xavier
 */
public class Migracions {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] tauless = new String[100000]; 
        String[] errors = new String[100000]; 
        int i = 0;
        int llargadaerrors = 0;
        int o = 0;
        int rr = 1;
        int rows = 0;
        int r2;
        boolean bool = false;
        boolean bool2 = true;
        boolean menu = true;
        boolean boolaoe = false;
        String ip = "";
        String cadenava = "";
        String variables = null;
        String user = "";
        String password = "";
        String bbdd = "";
        String bbddmsql = "";
        String connexio = "";
        String taula = "";
        String variablecon = "";
        ResultSet re;
        String nom = "";
        int value = 0;
        do 
        {
        r2 = 10000;
        if(menu == true) 
        {
        System.out.println("------------------MENU---------------------");
        System.out.println("1. Migració de dades de Access a sql Automàtica");
        System.out.println("8. Requisits del programa (LLEGEIXME)");
        System.out.println("-------------------------------------------");
        }
        String cadena = "";
        if(bool2 == true && menu == true) 
        {
            cadena = br.readLine();
            value = Integer.parseInt(cadena);
        }
        if(menu == false && bool2 == true) 
        {
            value = 1;
        }
        if(bool2 == false) 
        {
            value = 4;
        }
        switch(value) 
        {
            case 1:
                if(bool == false) 
                {
                System.out.println("Escriu tots els valors per comes (bbdd,(mysql) Ip Servidor, Usuari, Contrasenya, bbdd)");
                cadena = br.readLine();
                variables = cadena;
                System.out.println("Escriu totes les taules per ,");
                cadenava = br.readLine();
                tauless = cadenava.split(",");
                for (int j = 0; j < tauless.length; j++) 
                {
                    System.out.println(tauless[j]);  
                }
                }
                String[] arrayvalors = variables.split(",");
                bbdd = arrayvalors[0];
                bbddmsql = arrayvalors[4];
                password = arrayvalors[3];
                user = arrayvalors[2];
                ip = arrayvalors[1];
                connexio = arrayvalors[0];
                Connection conn = DriverManager.getConnection("jdbc:ucanaccess://"+ arrayvalors[0] +".mdb");
                Statement st = conn.createStatement();
                if(menu == false && bool2 == true) 
                {
                    o++;
                }
                try 
                {
                    Statement s = conn.createStatement();
                    if(tauless[o].contains("�")) 
                    {
                        llargadaerrors++;
                        errors[llargadaerrors] = "La taula " +tauless[o] + " No ha pogut ser migrada ja que conté un accent";
                        bool = true;
                        bool2 = true;
                        menu = false;
                        break;
                    }
                    re = st.executeQuery("select * from "+tauless[o]);
                    re.next();
                    ResultSetMetaData rsm;
                    if(bool2 == true) 
                    {
                    for (rr = 1; rr < r2 ; rr++) 
                    {
                        if(re.getString(rr) == null) 
                        {
                            System.out.println("null");
                        }
                        if(re.getString(rr) != "null" && re.getString(rr) != "FALSE" && re.getString(rr) != "null")
                        {
                            System.out.println(re.getString(rr));
                        }
                        if(re.getString(rr) == "FALSE") 
                        {
                            System.out.println("false");
                            System.out.println(rr);
                        }
                        if(re.getString(rr) == "TRUE") 
                        {
                            System.out.println("true");
                            System.out.println(rr);
                        }
                    }
                    }
                }
                catch(Exception e) 
                {
                    r2 = rr -3;
                    bool2 = false;
                    System.out.println(rr);
                }
                finally 
                {
                }
                break;
                case 4:
                    try 
                    {
                    conn = DriverManager.getConnection("jdbc:ucanaccess://"+ connexio +".mdb");
                    st = conn.createStatement();
                    re = st.executeQuery("SELECT COUNT(*) FROM "+tauless[o]);
                    bool = true;
                    re.next();
                    int i2 = 1;
                    int pos2 = 0;
                    int rows2 = re.getInt(1) -1;
                    String valor = "";
                    String[] forma2 = new String[99999999];
                    re = st.executeQuery("SELECT * FROM "+tauless[o]);
                    re.next();
                    for (pos2 = 0; pos2 < rows2; ) 
                    {
                        if(i2 == rr) 
                        {
                        pos2++;
                        re.next();
                        i2 = 1;
                        }
                        valor = re.getString(i2);
                        forma2[pos2] += valor;
                        forma2[pos2] += ";";
                        forma2[pos2].replace(',', '.');
                        System.out.println(forma2[pos2]);
                        i2++;
                    }
                    st.close();
                    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("variables.txt")));
                    for (int j = 0; j < forma2.length; j++)
                    {
                        if(forma2[j] != null) 
                        {
                            String replaceAll = forma2[j].replaceAll("null", "");
                            out.write(replaceAll);
                            out.println("");
                            out.flush();
                        }
                    }
                    out.close();
                    System.out.println("Les variables estàn guardades en el fitxer de text");
                    bool = true;
                    bool2 = true;
                    menu = false;
                Connection connection = DriverManager.getConnection("jdbc:mysql://"+ip+"/?user="+user+"&password="+password);
                PreparedStatement ps = connection.prepareStatement("use "+bbddmsql);
                int result = ps.executeUpdate();
                ps = connection.prepareStatement("DELETE FROM " +tauless[o]);
                result = ps.executeUpdate();
                ps = connection.prepareStatement("SET SESSION sql_mode = ''");
                result = ps.executeUpdate();
                ps = connection.prepareStatement("LOAD DATA LOCAL INFILE 'variables.txt' REPLACE INTO TABLE `"+tauless[o]+"` FIELDS TERMINATED BY ';' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n';");
                result = ps.executeUpdate();
                result = ps.executeUpdate();
                }
                catch (ArrayIndexOutOfBoundsException e) 
                {
                        rr--;
                        llargadaerrors++;
                        errors[llargadaerrors] = "La taula " +tauless[o] + " S'ha excedit de columnes, (s'ha reparat sol)";
                }
                catch (SQLException ex) 
                {
                    llargadaerrors++;
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                    errors[llargadaerrors] = "[MYSQL]" + ex.getMessage() + ex.getSQLState() + ex.getErrorCode();
                }
                break;
                case 8:
                    System.out.println("--------------------------------LLEGEIXME----------------------------------------------------------");
                    System.out.println("Migracions Mysql");
                    System.out.println("Programa creat per: Xavier Flores Mestre en l'any 2017");
                    System.out.println("Es permet ús particular sense comercialització");
                    System.out.println("Requisits:");
                    System.out.println("1. Tenir les taules sense accents");
                    System.out.println("2. Un mínim de 2 rows i 2 columnes per taules (En càs de ser menor la taula no serà pujada)");
                    System.out.println("3. Col.locar les BBDDS de ACCESS en el mateix fitxer que està el programa");
                    System.out.println("4. Tenir el fitxer valors.txt en el mateix fitxer del programa");
                    System.out.println("5. Tenir totes les llibreries del java per funcionar correctament (ACCESS, Mysql)");
                    System.out.println("--------------------------------LLEGEIXME----------------------------------------------------------");
                    break;
            } 
        }
        while(o != tauless.length);
        System.out.println("--------------Informe d'errors------------------");
        for (int j = 0; j < llargadaerrors; j++) 
        {
            System.out.println("ID Error: " + j +" Descripcio "+errors[llargadaerrors]);
        }
        System.out.println("--------------Informe d'errors------------------");
    }
}

