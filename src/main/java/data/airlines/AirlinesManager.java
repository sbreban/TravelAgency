package data.airlines;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AirlinesManager {
  private Connection connection;

  public AirlinesManager() {
    // create a database connection
    try {
      connection = DriverManager.getConnection("jdbc:sqlite:airlines.db");
    } catch (SQLException e) {
      System.err.println(e);
    }
  }

  public List<Flight> getAllFlights() {
    PreparedStatement statement = null;
    ResultSet rs = null;
    List<Flight> flights = new ArrayList<>();
    try {
      String query = "SELECT " +
          "  r.id, " +
          "  r.source, " +
          "  r.destination, " +
          "  f.departure_time, " +
          "  f.arrival_time " +
          "FROM flights f " +
          "  INNER JOIN routes r ON f.route_id = r.id";
      statement = connection.prepareStatement(query);
      rs = statement.executeQuery();
      while (rs.next()) {
        // read the result set
        Flight flight = extractFlight(rs);

        flights.add(flight);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
    return flights;
  }

  public List<Flight> getFlights(String toDestination) {
    PreparedStatement statement = null;
    ResultSet rs = null;
    List<Flight> flights = new ArrayList<>();
    try {
      String query = "SELECT " +
          "  r.id, " +
          "  r.source, " +
          "  r.destination, " +
          "  f.departure_time, " +
          "  f.arrival_time " +
          "FROM flights f " +
          "  INNER JOIN routes r ON f.route_id = r.id " +
          "WHERE r.destination = ?";
      statement = connection.prepareStatement(query);
      statement.setString(1, toDestination);
      rs = statement.executeQuery();
      while (rs.next()) {
        // read the result set
        Flight flight = extractFlight(rs);

        flights.add(flight);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
    return flights;
  }

  private Flight extractFlight(ResultSet rs) throws SQLException {
    int routeId = rs.getInt(1);
    String source = rs.getString(2);
    String destination = rs.getString(3);
    Timestamp departure = rs.getTimestamp(4);
    Timestamp arrival = rs.getTimestamp(5);
    Route route = new Route(routeId, source, destination);
    return new Flight(route, departure, arrival);
  }

  public Route getRoute(int routeId) {
    PreparedStatement statement = null;
    ResultSet rs = null;
    Route route = null;
    try {
      String query = "SELECT * FROM routes WHERE id = ?";
      statement = connection.prepareStatement(query);
      statement.setInt(1, routeId);
      rs = statement.executeQuery();
      while (rs.next()) {
        // read the result set
        int id = rs.getInt(1);
        String source = rs.getString(2);
        String destination = rs.getString(3);

        route = new Route(id, source, destination);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
    return route;
  }

  public void addRoute(Route route) {
    PreparedStatement statement = null;
    try {
      String query = "INSERT INTO routes (id, source, destination) VALUES (?, ?, ?)";
      statement = connection.prepareStatement(query);
      statement.setInt(1, route.getId());
      statement.setString(2, route.getSource());
      statement.setString(3, route.getDestination());
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
  }

  public void removeRoute(int routeId) {
    PreparedStatement statement = null;
    try {
      String query = "DELETE FROM routes " +
          "WHERE id = ?";
      statement = connection.prepareStatement(query);
      statement.setInt(1, routeId);
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
  }

  public void addFlight(Flight flight) {
    PreparedStatement statement = null;
    try {
      String query = "INSERT INTO flights (route_id, departure_time, arrival_time) VALUES (?, ?, ?)";
      statement = connection.prepareStatement(query);
      statement.setInt(1, flight.getRoute().getId());
      statement.setTimestamp(2, flight.getDeparture());
      statement.setTimestamp(3, flight.getArrival());
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
  }

  public void removeFlight(Flight flight) {
    PreparedStatement statement = null;
    try {
      String query = "DELETE FROM flights WHERE route_id = ? and departure_time = ? and arrival_time = ?";
      statement = connection.prepareStatement(query);
      statement.setInt(1, flight.getRoute().getId());
      statement.setTimestamp(2, flight.getDeparture());
      statement.setTimestamp(3, flight.getArrival());
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
  }

  public void close() {
    try {
      if (connection != null)
        connection.close();
    } catch (SQLException e) {
      // connection close failed.
      System.err.println(e);
    }
  }
}
