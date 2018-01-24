package server;

import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import transactionmanager.TransactionManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class TravelAgencyServer {

  private static final String FILENAME = "transactions";
  private static final Logger logger = Logger.getLogger(TravelAgencyServer.class.getName());

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = ServerBuilder.forPort(port)
        .addService(new TransactionHandlerImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        TravelAgencyServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  static class TransactionHandlerImpl extends TransactionHandlerGrpc.TransactionHandlerImplBase {

    @Override
    public void sendTransaction(TransactionRequest req, StreamObserver<TransactionReply> responseObserver) {
      TransactionReply reply = TransactionReply.newBuilder().setMessage("Hello " + req.getTransaction()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  public static void main(String[] args) throws InterruptedException {

    try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

      String sCurrentLine = br.readLine();
      int noTransactions = Integer.parseInt(sCurrentLine);
      Set<Variable> variables = new HashSet<>();
      List<Transaction> transactions = new ArrayList<>();

      for (int noTransaction = 0; noTransaction < noTransactions; noTransaction++) {
        sCurrentLine = br.readLine();
        int noInstructions = Integer.parseInt(sCurrentLine.split(" ")[0]);
        String transactionId = sCurrentLine.split(" ")[1];

        Set<Variable> readSet = new HashSet<>();
        Set<Variable> writeSet = new HashSet<>();
        List<Operation> operations = new ArrayList<>();
        for (int noInstruction = 0; noInstruction < noInstructions; noInstruction++) {
          sCurrentLine = br.readLine();
          String[] lineElements = sCurrentLine.split(" ");
          String instruction = lineElements[0];
          String var = lineElements[1];

          Variable variable = Variable.newBuilder().setId(var).build();
          variables.add(variable);

          OperationParameters parameters = OperationParameters.newBuilder().build();
          if (lineElements.length > 2) {
            List<String> stringListParameters = new ArrayList<>();
            for (int parameterIndex = 2; parameterIndex < lineElements.length; parameterIndex++) {
              stringListParameters.add(lineElements[parameterIndex]);
            }
            parameters = OperationParameters.newBuilder().addAllParameters(stringListParameters).build();
          }
          Operation operation = Operation.newBuilder().setInstruction(instruction).setVariable(variable).
              setParameters(parameters).build();

          if (operation.getInstruction().equals("W") || operation.getInstruction().equals("D")) {
            writeSet.add(operation.getVariable());
          } else if (operation.getInstruction().equals("R")) {
            readSet.add(operation.getVariable());
          }

          operations.add(operation);
        }

        Transaction transaction = Transaction.newBuilder().setId(transactionId).
            addAllOperations(operations).
            addAllReadSet(readSet).
            addAllWriteSet(writeSet).
            build();
        transactions.add(transaction);
      }

      for (Transaction transaction : transactions) {
        System.out.println(TextFormat.shortDebugString(transaction) + " " + transaction.getOperationsList());
      }

      TransactionManager transactionManager = new TransactionManager();
      transactionManager.setTransactions(transactions);
      transactionManager.setVariables(variables);
      transactionManager.run();

      final TravelAgencyServer server = new TravelAgencyServer();
      server.start();
      server.blockUntilShutdown();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
