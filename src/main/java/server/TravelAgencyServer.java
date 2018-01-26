package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import transactionmanager.TransactionManager;

import java.io.IOException;
import java.util.logging.Logger;

public class TravelAgencyServer {

  private static final Logger logger = Logger.getLogger(TravelAgencyServer.class.getName());

  private TransactionManager transactionManager;
  private Server server;

  public TravelAgencyServer(TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = ServerBuilder.forPort(port)
        .addService(new TransactionHandlerImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      TravelAgencyServer.this.stop();
      System.err.println("*** server shut down");
    }));
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

  class TransactionHandlerImpl extends TransactionHandlerGrpc.TransactionHandlerImplBase {
    @Override
    public void sendTransaction(TransactionRequest req, StreamObserver<TransactionReply> responseObserver) {
      TravelAgencyServer.this.transactionManager.addTransaction(req.getTransaction(0), responseObserver);
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {

    TransactionManager transactionManager = new TransactionManager();
    final TravelAgencyServer server = new TravelAgencyServer(transactionManager);
    server.start();
    server.blockUntilShutdown();

  }
}
