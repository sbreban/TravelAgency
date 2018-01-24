package transactionmanager;

import airlines.AirlinesManager;
import airlines.Flight;
import airlines.Route;
import hotels.Hotel;
import hotels.HotelsManager;
import io.grpc.stub.StreamObserver;
import server.*;
import users.User;
import users.UsersManager;

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
    List<Operation> reverseOperations = new ArrayList<>();
    StringBuilder messageBuilder = new StringBuilder();

    try {
      while (true) {
        try {
          reentrantLock.lock();
          for (Variable variable : write) {
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

        messageBuilder.append("Success!");
        for (Operation operation : transaction.getOperationsList()) {
          runOperation(operation, messageBuilder, reverseOperations);
        }
        System.out.println("All operations run for " + transaction.getId() + " at " + new Date(System.currentTimeMillis()));

        releaseLocks(transaction);

        TransactionReply reply = TransactionReply.newBuilder().setMessage(messageBuilder.toString()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        System.out.println("All locks released from " + transaction.getId());
        break;
      }
    } catch (Exception e) {
      for (Operation reverserOperation : reverseOperations) {
        runOperation(reverserOperation, messageBuilder, null);
      }
      releaseLocks(transaction);
      TransactionReply reply = TransactionReply.newBuilder().setMessage(messageBuilder.toString()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
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

  private void runOperation(Operation operation, StringBuilder messageBuilder, List<Operation> reverseOperations) {
    AirlinesManager airlinesManager = new AirlinesManager();
    HotelsManager hotelsManager = new HotelsManager();
    UsersManager usersManager = new UsersManager();

    Operation reverseOperation = null;
    if (operation.getInstruction().equals("R") && operation.getVariable().getId().equals("flights")) {
      List<Flight> flights = airlinesManager.getAllFlights();
      messageBuilder.append(flights);
    } else if (operation.getInstruction().equals("W") && operation.getVariable().getId().equals("route")) {
      OperationParameters parameters = operation.getParameters();
      Route route = new Route(Integer.parseInt(parameters.getParameters(0)), parameters.getParameters(1), parameters.getParameters(2));
      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(route.getId() + "").build()).
            build();
        reverseOperations.add(reverseOperation);
      }
      airlinesManager.addRoute(route);
      if (parameters.getParametersCount() > 3) {
        throw new IllegalArgumentException();
      }
    } else if (operation.getInstruction().equals("D") && operation.getVariable().getId().equals("route")) {
      OperationParameters parameters = operation.getParameters();
      int routeId = Integer.parseInt(parameters.getParameters(0));
      airlinesManager.removeRoute(routeId);
    } else if (operation.getInstruction().equals("R") && operation.getVariable().getId().equals("destinations")) {
      List<Flight> flights = airlinesManager.getFlights(operation.getParameters().getParameters(0));
      messageBuilder.append(flights);
    } else if (operation.getInstruction().equals("R") && operation.getVariable().getId().equals("hotels")) {
      List<Hotel> hotels = hotelsManager.getAllHotels();
      messageBuilder.append(hotels);
    } else if (operation.getInstruction().equals("R") && operation.getVariable().getId().equals("users")) {
      List<User> users = usersManager.getAllUsers();
      messageBuilder.append(users);
    } else if (operation.getInstruction().equals("W") && operation.getVariable().getId().equals("user")) {
      OperationParameters parameters = operation.getParameters();
      User user = new User(Integer.parseInt(parameters.getParameters(0)), parameters.getParameters(1), Integer.parseInt(parameters.getParameters(2)));
      usersManager.addUser(user);
      messageBuilder.append("User ").append(user.getName()).append(" added!");
      if (reverseOperations != null) {
        reverseOperation = Operation.newBuilder().
            setVariable(operation.getVariable()).
            setInstruction("D").
            setParameters(OperationParameters.newBuilder().addParameters(user.getId() + "").build()).
            build();
        reverseOperations.add(reverseOperation);
      }
    }

    airlinesManager.close();
    hotelsManager.close();
    usersManager.close();
  }
}
