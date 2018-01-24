package client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import server.Transaction;
import server.TransactionHandlerGrpc;
import server.TransactionReply;
import server.TransactionRequest;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a greeting from the {@link server.TravelAgencyServer}.
 */
public class TravelAgencyClient {
  private static final Logger logger = Logger.getLogger(TravelAgencyClient.class.getName());

  private final ManagedChannel channel;
  private final TransactionHandlerGrpc.TransactionHandlerBlockingStub blockingStub;

  /**
   * Construct client connecting to HelloWorld server at {@code host:port}.
   */
  public TravelAgencyClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext(true)
        .build());
  }

  /**
   * Construct client for accessing RouteGuide server using the existing channel.
   */
  TravelAgencyClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = TransactionHandlerGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /**
   * Say hello to server.
   */
  public void greet(Transaction transaction) {
    logger.info("Will try to greet " + transaction + " ...");
    TransactionRequest request = TransactionRequest.newBuilder().setTransaction(transaction).build();
    TransactionReply response;
    try {
      response = blockingStub.sendTransaction(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Greeting: " + response.getMessage());
  }

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting.
   */
  public static void main(String[] args) throws Exception {
    TravelAgencyClient client = new TravelAgencyClient("localhost", 50051);
    try {
      /* Access a service running on the local machine on port 50051 */
      String user = "world";
      if (args.length > 0) {
        user = args[0]; /* Use the arg as the name to greet if provided */
      }
      client.greet(Transaction.newBuilder().setId("1").build());
    } finally {
      client.shutdown();
    }
  }
}
