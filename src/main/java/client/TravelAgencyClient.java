package client;

import com.google.protobuf.TextFormat;
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TravelAgencyClient {
  private static final Logger logger = Logger.getLogger(TravelAgencyClient.class.getName());

  private final ManagedChannel channel;
  private final TransactionHandlerGrpc.TransactionHandlerBlockingStub blockingStub;
  private static final String FILENAME = "transactions";

  private TravelAgencyClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext(true)
        .build());
  }

  private TravelAgencyClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = TransactionHandlerGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  private void sendTransactions(List<Transaction> transactions) {
    for (Transaction transaction : transactions) {
      logger.info("Will try to sendTransactions " + transactions + " ...");
      TransactionRequest request = TransactionRequest.newBuilder().addTransaction(transaction).build();
      TransactionReply response;
      try {
        response = blockingStub.sendTransaction(request);
      } catch (StatusRuntimeException e) {
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        return;
      }
      logger.info("Reply: " + response.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    TravelAgencyClient client = new TravelAgencyClient("localhost", 50051);

    List<Transaction> transactions = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

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

      for (Transaction transaction : transactions) {
        System.out.println(TextFormat.shortDebugString(transaction) + " " + transaction.getOperationsList());
      }

      client.sendTransactions(transactions);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      client.shutdown();
    }
  }
}
