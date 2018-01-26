package client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import server.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TravelAgencyClient {
  private static final Logger logger = Logger.getLogger(TravelAgencyClient.class.getName());

  private final ManagedChannel channel;
  private final TransactionHandlerGrpc.TransactionHandlerBlockingStub blockingStub;
  private final Transaction transaction;
  private static final ExecutorService executor = Executors.newFixedThreadPool(5);


  private TravelAgencyClient(String host, int port, Transaction transaction) {
    this(ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext(true)
        .build(), transaction);
  }

  private TravelAgencyClient(ManagedChannel channel, Transaction transaction) {
    this.channel = channel;
    this.blockingStub = TransactionHandlerGrpc.newBlockingStub(channel);
    this.transaction = transaction;
  }

  private void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  private void sendTransactions() {
    logger.info("Will try to send transaction " + transaction.getId() + " ...");
    TransactionRequest request = TransactionRequest.newBuilder().addTransaction(transaction).build();
    TransactionReply response;
    try {
      response = blockingStub.sendTransaction(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Reply for transaction " + transaction.getId() + ": " + response.getMessage());
    try {
      shutdown();
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) {

    List<Transaction> transactions = readTransactionsFromFile(args[0]);

    int port = 50051;
    for (Transaction transaction : transactions) {
      TravelAgencyClient client = new TravelAgencyClient("localhost", port, transaction);
      executor.submit(client::sendTransactions);
    }

  }

  private static List<Transaction> readTransactionsFromFile(String transactionFile) {
    List<Transaction> transactions = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(transactionFile))) {

      String sCurrentLine = br.readLine();
      int noTransactions = Integer.parseInt(sCurrentLine);
      Set<Variable> variables = new HashSet<>();

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

    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
    return transactions;
  }
}
