package transactionmanager;

import data.OperationException;
import data.airlines.AirlinesManager;
import data.airlines.Flight;
import data.airlines.Route;
import data.hotels.Hotel;
import data.hotels.HotelsManager;
import data.users.HotelReservation;
import io.grpc.stub.StreamObserver;
import server.*;
import data.users.User;
import data.users.UsersManager;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {

  private Map<Variable, Transaction> writeLocks = new HashMap<>();
  private Map<Variable, List<Transaction>> readLocks = new HashMap<>();

  private ExecutorService executor = Executors.newFixedThreadPool(5);
  private ReentrantLock reentrantLock = new ReentrantLock();

  public void addTransaction(Transaction transaction, StreamObserver<TransactionReply> responseObserver) {
    executor.submit(() -> runTransaction(transaction, responseObserver));
  }

  private void runTransaction(Transaction transaction, StreamObserver<TransactionReply> responseObserver) {
    List<Variable> read = transaction.getReadSetList();
    List<Variable> write = transaction.getWriteSetList();
    Set<Variable> writeAndRead = new HashSet<>();
    writeAndRead.addAll(write);
    writeAndRead.addAll(read);
    List<Operation> reverseOperations = new LinkedList<>();
    StringBuilder messageBuilder = new StringBuilder();
    Map<Variable, List<Operation>> variableToReadOperation = new HashMap<>();
    Map<Variable, List<Operation>> variableToWriteOperation = new HashMap<>();

    try {
      while (true) {
        try {
          reentrantLock.lock();
          for (Variable variable : writeAndRead) {
            if (writeLocks.get(variable) != null) {
              throw new IllegalAccessException();
            }
          }
        } catch (IllegalAccessException e) {
          reentrantLock.unlock();
          continue;
        }

        for (Variable variable : write) {
          writeLocks.put(variable, transaction);
        }

        for (Variable variable : read) {
          readLocks.computeIfAbsent(variable, k -> new ArrayList<>());
          readLocks.get(variable).add(transaction);
        }
        reentrantLock.unlock();
        System.out.println("All locks for " + transaction.getId() + " acquired at " + new Date(System.currentTimeMillis()));

        for (Operation operation : transaction.getOperationsList()) {
          if (isReadOperation(operation)) {
            List<Operation> operationsForRead = variableToReadOperation.computeIfAbsent(operation.getVariable(), k -> new ArrayList<>());
            operationsForRead.add(operation);
          } else {
            List<Operation> operationsForWrite = variableToWriteOperation.computeIfAbsent(operation.getVariable(), k -> new ArrayList<>());
            operationsForWrite.add(operation);
          }
        }

        for (Operation operation : transaction.getOperationsList()) {
          runOperation(operation, messageBuilder, reverseOperations);

          Variable operationVariable = operation.getVariable();
          if (isReadOperation(operation)) {
            variableToReadOperation.get(operationVariable).remove(operation);
            if (variableToReadOperation.get(operationVariable).size() == 0) {
              reentrantLock.lock();
              if (readLocks.get(operationVariable) != null && readLocks.get(operationVariable).contains(transaction)) {
                readLocks.get(operationVariable).remove(transaction);
              }
              reentrantLock.unlock();
            }
          } else {
            variableToWriteOperation.get(operationVariable).remove(operation);
            if (variableToWriteOperation.get(operationVariable).size() == 0) {
              reentrantLock.lock();
              if (writeLocks.get(operationVariable) != null && writeLocks.get(operationVariable) == transaction) {
                writeLocks.put(operationVariable, null);
              }
              reentrantLock.unlock();
            }
          }
        }
        messageBuilder.append("Success!");

        System.out.println("All operations run for " + transaction.getId() + " at " + new Date(System.currentTimeMillis()));

        sendReply(responseObserver, messageBuilder);
        System.out.println("All locks released from " + transaction.getId());
        break;
      }
    } catch (OperationException | IllegalArgumentException e) {
      String transactionFailMessage = "Transaction " + transaction.getId() + " failed at " + new Date(System.currentTimeMillis());
      System.out.println(transactionFailMessage + ". Rollback!");

      try {
        for (int i = reverseOperations.size() - 1; i >= 0; i--) {
          Operation reverserOperation = reverseOperations.get(i);
          runOperation(reverserOperation, messageBuilder, null);
        }
      } catch (OperationException rollbackException) {
        System.err.println(rollbackException.getMessage());
      }
      releaseLocks(transaction);
      messageBuilder = new StringBuilder();
      messageBuilder.append(transactionFailMessage);
      sendReply(responseObserver, messageBuilder);
    }
  }

  private boolean isReadOperation(Operation operation) {
    return operation.getInstruction().equals("R");
  }

  private void sendReply(StreamObserver<TransactionReply> responseObserver, StringBuilder messageBuilder) {
    TransactionReply reply = TransactionReply.newBuilder().setMessage(messageBuilder.toString()).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  private void releaseLocks(Transaction transaction) {
    reentrantLock.lock();
    for (Variable variable : writeLocks.keySet()) {
      if (writeLocks.get(variable) != null && writeLocks.get(variable) == transaction) {
        writeLocks.put(variable, null);
      }
      if (readLocks.get(variable) != null && readLocks.get(variable).contains(transaction)) {
        readLocks.get(variable).remove(transaction);
      }
    }
    reentrantLock.unlock();
  }

  private void runOperation(Operation operation, StringBuilder messageBuilder, List<Operation> reverseOperations) throws OperationException {
    AirlinesManager airlinesManager = new AirlinesManager();
    HotelsManager hotelsManager = new HotelsManager();
    UsersManager usersManager = new UsersManager();

    if (isRouteOperation(operation)) {
      handleRouteOperation(operation, airlinesManager, reverseOperations, messageBuilder);
    } else if (isFlightOperation(operation)) {
      handleFlightOperation(operation, airlinesManager, reverseOperations, messageBuilder);
    } else if (isHotelOperation(operation)) {
      handleHotelOperations(operation, hotelsManager, reverseOperations, messageBuilder);
    } else if (isUserOperation(operation)) {
      handleUserOperation(operation, usersManager, reverseOperations, messageBuilder);
    } else if (isHotelReservationOperation(operation)) {
      handleHotelReservationOperation(operation, usersManager, reverseOperations, messageBuilder);
    }

    airlinesManager.close();
    hotelsManager.close();
    usersManager.close();
  }

  private void handleHotelReservationOperation(Operation operation, UsersManager usersManager, List<Operation> reverseOperations,
                                               StringBuilder messageBuilder) throws OperationException {
    Operation reverseOperation;
    if (isReadOperation(operation)) {

    } else if (operation.getInstruction().equals("W")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 5) {
        throw new IllegalArgumentException();
      }

      int userId = Integer.parseInt(parameters.getParameters(0));
      int hotelId = Integer.parseInt(parameters.getParameters(1));
      Timestamp arrival = new Timestamp(Long.parseLong(parameters.getParameters(2)));
      Timestamp departure = new Timestamp(Long.parseLong(parameters.getParameters(3)));
      int noRooms = Integer.parseInt(parameters.getParameters(4));

      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(userId + "").
                addParameters(hotelId + "").
                build()).
            build();
        reverseOperations.add(reverseOperation);
      }
      usersManager.reserveHotel(new HotelReservation(userId, hotelId, arrival, departure, noRooms));
      messageBuilder.append("User ").append(userId).append(" reserved a room at hotel ").append(hotelId).append("\n");
    } else if (operation.getInstruction().equals("D")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 2) {
        throw new IllegalArgumentException();
      }

      int userId = Integer.parseInt(parameters.getParameters(0));
      int hotelId = Integer.parseInt(parameters.getParameters(1));

      HotelReservation hotelReservation = usersManager.getHotelReservation(userId, hotelId);
      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("W").
            setParameters(OperationParameters.newBuilder().addParameters(userId + "").
                addParameters(hotelId + "").
                addParameters(hotelReservation.getArrival().getTime() + "").
                addParameters(hotelReservation.getDeparture().getTime() + "").
                addParameters(hotelReservation.getNoRooms() + "").
                build()).
            build();
        reverseOperations.add(reverseOperation);
      }
      usersManager.removeHotelReservation(userId, hotelId);
      messageBuilder.append("User ").append(userId).append(" removed reserved rooms at hotel ").append(hotelId).append("\n");
    }
  }

  private void handleUserOperation(Operation operation, UsersManager usersManager, List<Operation> reverseOperations,
                                   StringBuilder messageBuilder) throws OperationException {
    Operation reverseOperation = null;
    if (isReadOperation(operation)) {
      List<User> users = usersManager.getAllUsers();
      messageBuilder.append(users).append("\n");
    } else if (operation.getInstruction().equals("W")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 3) {
        throw new IllegalArgumentException();
      }

      User user = new User(Integer.parseInt(parameters.getParameters(0)), parameters.getParameters(1), Integer.parseInt(parameters.getParameters(2)));
      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(user.getId() + "").build()).
            build();
      }
      usersManager.addUser(user);
      if (reverseOperation != null) {
        reverseOperations.add(reverseOperation);
      }
      messageBuilder.append("User ").append(user.getName()).append(" added!").append("\n");
    }
  }

  private void handleHotelOperations(Operation operation, HotelsManager hotelsManager, List<Operation> reverseOperations,
                                     StringBuilder messageBuilder) {
    if (isReadOperation(operation)) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() == 0) {
        List<Hotel> hotels = hotelsManager.getAllHotels();
        messageBuilder.append(hotels).append("\n");
      } else if (parameters.getParametersCount() == 1) {
        List<Hotel> hotels = hotelsManager.getHotels(parameters.getParameters(0));
        messageBuilder.append(hotels).append("\n");
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  private void handleFlightOperation(Operation operation, AirlinesManager airlinesManager, List<Operation> reverseOperations,
                                     StringBuilder messageBuilder) {
    Operation reverseOperation = null;
    if (isReadOperation(operation)) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() == 0) {
        List<Flight> flights = airlinesManager.getAllFlights();
        messageBuilder.append(flights).append("\n");
      } else if (parameters.getParametersCount() == 1) {
        List<Flight> flights = airlinesManager.getFlights(parameters.getParameters(0));
        messageBuilder.append(flights).append("\n");
      } else {
        throw new IllegalArgumentException();
      }
    } else if (operation.getInstruction().equals("W")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 3) {
        throw new IllegalArgumentException();
      }

      int routeId = Integer.parseInt(parameters.getParameters(0));
      Timestamp departure = new Timestamp(Long.parseLong(parameters.getParameters(1)));
      Timestamp arrival = new Timestamp(Long.parseLong(parameters.getParameters(2)));

      Flight flight = new Flight(routeId, departure, arrival);

      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(flight.getRoute().getId() + "").
                addParameters(flight.getDeparture().getTime() + "").
                addParameters(flight.getDeparture().getTime() + "")).build();
      }
      airlinesManager.addFlight(flight);
      if (reverseOperation != null) {
        reverseOperations.add(reverseOperation);
      }
      messageBuilder.append("Flight successfully added!\n");
    } else if (operation.getInstruction().equals("D")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 3) {
        throw new IllegalArgumentException();
      }

      int routeId = Integer.parseInt(parameters.getParameters(0));
      Timestamp departure = new Timestamp(Long.parseLong(parameters.getParameters(1)));
      Timestamp arrival = new Timestamp(Long.parseLong(parameters.getParameters(2)));

      Flight flight = new Flight(routeId, departure, arrival);

      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("W").
            setParameters(OperationParameters.newBuilder().addParameters(flight.getRoute().getId() + "").
                addParameters(flight.getDeparture().getTime() + "").
                addParameters(flight.getDeparture().getTime() + "")).build();
      }
      airlinesManager.removeFlight(flight);
      if (reverseOperation != null) {
        reverseOperations.add(reverseOperation);
      }
      messageBuilder.append("Flight successfully removed!\n");
    }
  }

  private void handleRouteOperation(Operation operation, AirlinesManager airlinesManager, List<Operation> reverseOperations,
                                    StringBuilder messageBuilder) {
    Operation reverseOperation = null;

    if (operation.getInstruction().equals("W")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 3) {
        throw new IllegalArgumentException();
      }

      Route route = new Route(Integer.parseInt(parameters.getParameters(0)), parameters.getParameters(1), parameters.getParameters(2));
      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(route.getId() + "").build()).
            build();
      }
      airlinesManager.addRoute(route);
      if (reverseOperation != null) {
        reverseOperations.add(reverseOperation);
      }
      messageBuilder.append("Route successfully added!\n");
    } else if (operation.getInstruction().equals("D")) {
      OperationParameters parameters = operation.getParameters();

      if (parameters.getParametersCount() != 1) {
        throw new IllegalArgumentException();
      }

      int routeId = Integer.parseInt(parameters.getParameters(0));

      if (reverseOperations != null) {
        Route toDeleteRoute = airlinesManager.getRoute(routeId);
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("W").
            setParameters(OperationParameters.newBuilder().addParameters(toDeleteRoute.getId() + "").
                addParameters(toDeleteRoute.getSource()).addParameters(toDeleteRoute.getDestination()).
                build()).
            build();
      }

      airlinesManager.removeRoute(routeId);
      if (reverseOperation != null) {
        reverseOperations.add(reverseOperation);
      }
      messageBuilder.append("Route successfully removed!\n");
    }
  }

  private boolean isFlightOperation(Operation operation) {
    return operation.getVariable().getId().equals("flights");
  }

  private boolean isRouteOperation(Operation operation) {
    return operation.getVariable().getId().equals("routes");
  }

  private boolean isHotelOperation(Operation operation) {
    return operation.getVariable().getId().equals("hotels");
  }

  private boolean isUserOperation(Operation operation) {
    return operation.getVariable().getId().equals("users");
  }

  private boolean isHotelReservationOperation(Operation operation) {
    return operation.getVariable().getId().equals("hotel_reservations");
  }

}
