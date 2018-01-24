package transactionmanager;

import airlines.AirlinesManager;
import airlines.Flight;
import airlines.Route;
import io.grpc.stub.StreamObserver;
import server.*;

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

      for (Operation operation : transaction.getOperationsList()) {
        runOperation(operation);
      }
      System.out.println("All operations run for " + transaction.getId() + " at " + new Date(System.currentTimeMillis()));

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

      TransactionReply reply = TransactionReply.newBuilder().setMessage("Success!").build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      System.out.println("All locks released from " + transaction.getId());
      break;
    }
  }

  private void runOperation(Operation operation) {
    if (operation.getInstruction().equals("R") && operation.getVariable().getId().equals("flights")) {
      AirlinesManager airlinesManager = new AirlinesManager();
      List<Flight> flights = airlinesManager.getAllFlights();
      System.out.println(flights);
      airlinesManager.close();
    } else if (operation.getInstruction().equals("W") && operation.getVariable().getId().equals("route")) {
      AirlinesManager airlinesManager = new AirlinesManager();
      OperationParameters parameters = operation.getParameters();
      Route route = new Route(Integer.parseInt(parameters.getParameters(0)), parameters.getParameters(1), parameters.getParameters(2));
      airlinesManager.addRoute(route);
      airlinesManager.close();
    } else if (operation.getInstruction().equals("D") && operation.getVariable().getId().equals("route")) {
      AirlinesManager airlinesManager = new AirlinesManager();
      OperationParameters parameters = operation.getParameters();
      int routeId = Integer.parseInt(parameters.getParameters(0));
      airlinesManager.removeRoute(routeId);
      airlinesManager.close();
    }
  }
}
