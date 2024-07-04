package com.pag.proyecto_integrador;


import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import org.json.JSONArray;
import org.json.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class App 
{
	
	
	public static void circuits() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\raw\\circuits.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO circuits (circuitId, circuitRef, name, location, country, lat, lng, alt, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int circuitId = Integer.parseInt(nextLine[0]);
                String circuitRef = nextLine[1];
                String name = nextLine[2];
                String location = nextLine[3];
                String country = nextLine[4];
                float lat = Float.parseFloat(nextLine[5]);
                float lng = Float.parseFloat(nextLine[6]);
                int alt = Integer.parseInt(nextLine[7]);
                String url = nextLine[8];

                statement.setInt(1, circuitId);
                statement.setString(2, circuitRef);
                statement.setString(3, name);
                statement.setString(4, location);
                statement.setString(5, country);
                statement.setFloat(6, lat);
                statement.setFloat(7, lng);
                statement.setInt(8, alt);
                statement.setString(9, url);

                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	

	private static Integer parseInteger(String value) {
	    return "N".equals(value) ? null : Integer.parseInt(value);
	}


	
    private static String parseString(String value) {
        return "N".equals(value) ? null : value;
    }

    //private static Date parseDate(String value) {
    //    return "N".equals(value) ? null : Date.valueOf(value);
   // }

    //private static Time parseTime(String value) {
    //    return "N".equals(value) ? null : Time.valueOf(value);
    //}
    
    private static Float parseFloat(String value) {
        return "N".equals(value) ? null : Float.parseFloat(value);
    }
	
	
	public static void races() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\raw\\races.csv";
        
    	Connection connection = null;
    	

    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO races (raceId, year, round, circuitId, name, date, time, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int raceId = parseInteger(nextLine[0]);
                int year = parseInteger(nextLine[1]);
                int round = parseInteger(nextLine[2]);
                int circuitId = parseInteger(nextLine[3]);
                String name = nextLine[4];
                String date = nextLine[5];
                String time = nextLine[6];
                String url = nextLine[7];

                statement.setInt(1, raceId);
                statement.setInt(2, year);
                statement.setInt(3, round);
                statement.setInt(4, circuitId);
                statement.setString(5, name);
                statement.setString(6, date);
                statement.setString(7, time);
                statement.setString(8, url);


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
		
	}
	
	public static void results() {
		System.out.println("Se ingresaron los datos de results");
	}
	
/*	
	public static void drivers()  throws CsvValidationException  {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "11032003";

	    String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\raw\\drivers.json"; // Cambia la ruta al archivo JSON

	    Connection connection = null;

	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO drivers (driverId, driverRef, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE driverRef=VALUES(driverRef), number=VALUES(number), code=VALUES(code), forename=VALUES(forename), surname=VALUES(surname), dob=VALUES(dob), nationality=VALUES(nationality), url=VALUES(url)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            try {
	                Integer driverId = parseInteger(nextLine[0]);
	                String driverRef = nextLine[1];
	                Integer number = parseInteger(nextLine[2]);
	                String code = nextLine[3];

	               
	                String nameJson = nextLine[4];
	                JSONObject nameObject = new JSONObject(nameJson);
	                String forename = nameObject.getString("forename");
	                String surname = nameObject.getString("surname");

	                String dob = nextLine[7];
	                String nationality = nextLine[8];
	                String url = nextLine[9];

	                if (driverId != null) {
	                    statement.setInt(1, driverId);
	                } else {
	                    statement.setNull(1, java.sql.Types.INTEGER);
	                }

	                statement.setString(2, driverRef);

	                if (number != null) {
	                    statement.setInt(3, number);
	                } else {
	                    statement.setNull(3, java.sql.Types.INTEGER);
	                }

	                statement.setString(4, code);
	                statement.setString(6, forename);
	                statement.setString(7, surname);

	                if (dob != null) {
	                    statement.setDate(8, java.sql.Date.valueOf(dob));
	                } else {
	                    statement.setNull(8, java.sql.Types.DATE);
	                }

	                statement.setString(9, nationality);
	                statement.setString(10, url);

	                statement.addBatch();
	            } catch (IllegalArgumentException e) {
	                System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
	            }
	        }

	        statement.executeBatch();
	        connection.commit();

	        System.out.println("Datos insertados correctamente en la base de datos.");
	    } catch (IOException | SQLException e) {
	        e.printStackTrace();
	        // Manejar errores
	    }
	}
*/
	
	public static void drivers() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "11032003";

	    String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\drivers.csv";

	    Connection connection = null;

	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO drivers (driverId, driverRef, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Skip header row

	        while ((nextLine = reader.readNext()) != null) {
	            int driverId = Integer.parseInt(nextLine[0]);
	            String driverRef = nextLine[1];
	            String numberStr = nextLine[2]; // Store as String first
	            String code = nextLine[3];
	            String forename = nextLine[4];
	            String surname = nextLine[5];
	            String dob = nextLine[6];
	            String nationality = nextLine[7];
	            String url = nextLine[8];

	            // Parse number or set to null if "N"
	            Integer number = parseInteger(numberStr);

	            statement.setInt(1, driverId);
	            statement.setString(2, driverRef);
	            statement.setObject(3, number, java.sql.Types.INTEGER); // Use setObject for nullable fields
	            statement.setString(4, code);
	            statement.setString(5, forename);
	            statement.setString(6, surname);
	            statement.setString(7, dob);
	            statement.setString(8, nationality);
	            statement.setString(9, url);

	            statement.addBatch();
	        }

	        reader.close();
	        statement.executeBatch();
	        connection.commit();
	        statement.close();
	        connection.close();

	        System.out.println("Datos insertados correctamente.");

	    } catch (IOException | SQLException e) {
	        e.printStackTrace();
	        try {
	            if (connection != null) {
	                connection.rollback();
	            }
	        } catch (SQLException ex) {
	            ex.printStackTrace();
	        }
	    }
	}	
	
/*	
	public static void drivers() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\drivers.csv";
        
    	Connection connection = null;
    	

    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO drivers (driverId, driverRef, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int driverId = Integer.parseInt(nextLine[0]);
                String driverRef = nextLine[1];
                int number = parseInteger(nextLine[2]);
                String code = nextLine[3];
                String forename = nextLine[4];
                String surname = nextLine[5];
                String dob = nextLine[6];
                String nationality = nextLine[7];
                String url = nextLine[8];
                
                statement.setInt(1, driverId);
                statement.setString(2, driverRef);

                statement.setInt(3, number);
                statement.setString(4, code);
                statement.setString(5, forename);
                statement.setString(6, surname);
                statement.setString(7, dob);
                statement.setString(8, nationality);
                statement.setString(9, url);


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
		
	}
	
	*/
	
	
	public static void constructors() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\constructors.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO constructors (constructorId, constructorRef, name, nationality, url) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int constructorId = Integer.parseInt(nextLine[0]);
                String constructorRef = nextLine[1];
                String name = nextLine[2];         
                String nationality = nextLine[3];
                String url = nextLine[4];
                
                statement.setInt(1, constructorId);
                statement.setString(2, constructorRef);
                statement.setString(3, name);
                statement.setString(4, nationality);
                statement.setString(5, url);


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	

	public static void constructor_standings() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\constructor_standings.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO constructor_standings (constructorStandingId, constructorId, points, positionText, wins) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int constructorStandingId = Integer.parseInt(nextLine[0]);
                int constructorId = Integer.parseInt(nextLine[1]);
                int points = Integer.parseInt(nextLine[2]);
                String positionText = nextLine[3];
                int wins = Integer.parseInt(nextLine[4]);

                
                statement.setInt(1, constructorStandingId);
                statement.setInt(2, constructorId);
                statement.setInt(3, points);
                statement.setString(4, positionText);
                statement.setInt(5, wins);


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	
	
	public static void lap_times() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\lap_times.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO lap_times (raceId, driverId, lap, position, time, milliseconds) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int circuitId = Integer.parseInt(nextLine[0]);
                int driverId = Integer.parseInt(nextLine[1]);
                int lap = Integer.parseInt(nextLine[2]);
                int position = Integer.parseInt(nextLine[3]);
                String time = nextLine[4];
                int milliseconds = Integer.parseInt(nextLine[5]);
                
                statement.setInt(1, circuitId);
                statement.setInt(2, driverId);
                statement.setInt(3, lap);
                statement.setInt(4, position);
                statement.setString(5, time);
                statement.setInt(6, milliseconds);


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	public static void pit_stops() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\pit_stops.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO pit_stops (raceId, driverId, stop, lap, time, duration, milliseconds) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int raceId = Integer.parseInt(nextLine[0]);
                int driverId = Integer.parseInt(nextLine[1]);
                int stop = Integer.parseInt(nextLine[2]);
                int lap = Integer.parseInt(nextLine[3]);
                String time = nextLine[4];
                String duration = nextLine[5];
                int milliseconds = Integer.parseInt(nextLine[6]);
                
                
                statement.setInt(1, raceId);
                statement.setInt(2, driverId);
                statement.setInt(3, stop);
                statement.setInt(4, lap);
                statement.setString(5, time);
                statement.setString(6, duration);
                statement.setInt(7, milliseconds);

                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
	public static void qualifying() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "11032003";
        
        
    	String path = "D:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\Proyecto_Act001\\CSV's\\qualifying.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO qualifying (qualifyId, raceId, driverId, constructorId, number, position, q1, q2, q3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
                int qualifyId = Integer.parseInt(nextLine[0]);
                int raceId = Integer.parseInt(nextLine[1]);
                int driverId = Integer.parseInt(nextLine[2]);
                int constructorId = Integer.parseInt(nextLine[3]);
                int number = Integer.parseInt(nextLine[4]);
                int position = Integer.parseInt(nextLine[5]);
                String q1 = nextLine[6];
                String q2 = nextLine[7];
                String q3 = nextLine[8];
                
                
                statement.setInt(1, qualifyId);
                statement.setInt(2, raceId);
                statement.setInt(3, driverId);
                statement.setInt(4, constructorId);
                statement.setInt(5, number);
                statement.setInt(6, position);
                statement.setString(7, q1);
                statement.setString(8, q2);
                statement.setString(9, q3);

                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	
    public static void main( String[] args ) throws CsvValidationException
    {
    	
    	//circuits();  LISTO
    	//races();  LISTO
    	//results();
    	//drivers(); LISTO
    	//constructors(); LISTO
    	//lap_times();  ERROR 1
    	//pit_stops(); Error 1 
    	//qualifying(); ERROR 1
    	
    	//constructor_standings(); ERROR 1
    	
//    	delete from circuits;
//    	select count(*) from circuits;
//    	select * from circuits;
    	
    	
    }
}
