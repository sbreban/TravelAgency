package data.airlines;

import java.sql.Timestamp;

public class Flight {
  private Route route;
  private Timestamp departure;
  private Timestamp arrival;

  public Flight(int routeId, Timestamp departure, Timestamp arrival) {
    this.route = new Route(routeId);
    this.departure = departure;
    this.arrival = arrival;
  }

  public Flight(Route route, Timestamp departure, Timestamp arrival) {
    this.route = route;
    this.departure = departure;
    this.arrival = arrival;
  }

  public Route getRoute() {
    return route;
  }

  public Timestamp getDeparture() {
    return departure;
  }

  public Timestamp getArrival() {
    return arrival;
  }

  @Override
  public String toString() {
    return "Flight{" +
        "route=" + route +
        ", departure=" + departure +
        ", arrival=" + arrival +
        '}';
  }
}
