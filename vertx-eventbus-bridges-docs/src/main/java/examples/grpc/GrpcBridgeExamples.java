package examples.grpc;

import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.streams.ReadStream;
import io.vertx.eventbus.bridge.grpc.GrpcEventBusBridge;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.event.v1alpha.EventBusBridgeGrpcClient;
import io.vertx.grpc.event.v1alpha.EventMessage;
import io.vertx.grpc.event.v1alpha.PublishMessageRequest;
import io.vertx.grpc.event.v1alpha.RequestMessageRequest;
import io.vertx.grpc.event.v1alpha.SendMessageRequest;
import io.vertx.grpc.event.v1alpha.SubscribeMessageRequest;
import io.vertx.grpc.event.v1alpha.UnsubscribeMessageRequest;

public class GrpcBridgeExamples {

  public void createServer(Vertx vertx) {
    // Configure bridge options
    BridgeOptions options = new BridgeOptions()
      .addInboundPermitted(new PermittedOptions().setAddress("hello"))
      .addInboundPermitted(new PermittedOptions().setAddress("echo"))
      .addOutboundPermitted(new PermittedOptions().setAddress("news"));

    // Create the bridge
    GrpcEventBusBridge bridge = GrpcEventBusBridge.create(
      vertx,
      options,
      7000,  // Port
      event -> {
        // Optional event handler for bridge events
        System.out.println("Bridge event: " + event.type());
        event.complete(true);
      }
    );

    // Start the bridge
    bridge.listen().onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("gRPC EventBus Bridge started");
      } else {
        System.err.println("Failed to start gRPC EventBus Bridge: " + ar.cause());
      }
    });
  }

  public void createClient(Vertx vertx) {
    // Create the gRPC client
    GrpcClient client = GrpcClient.client(vertx);
    SocketAddress socketAddress = SocketAddress.inetSocketAddress(7000, "localhost");
    EventBusBridgeGrpcClient grpcClient = EventBusBridgeGrpcClient.create(client, socketAddress);
  }

  public void sendMessage(EventBusBridgeGrpcClient grpcClient) {
    // Create a message
    JsonObject message = new JsonObject().put("value", "Hello from gRPC client");

    // Convert to Protobuf Struct
    Struct messageBody = jsonToStruct(message);

    // Create the request
    SendMessageRequest request = SendMessageRequest.newBuilder()
      .setAddress("hello")
      .setBody(messageBody)
      .build();

    // Send the message
    grpcClient.send(request).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("Message sent successfully");
      } else {
        System.err.println("Failed to send message: " + ar.cause());
      }
    });
  }

  public void requestResponse(EventBusBridgeGrpcClient grpcClient) {
    // Create a message
    JsonObject message = new JsonObject().put("value", "Hello from gRPC client");

    // Convert to Protobuf Struct
    Struct messageBody = jsonToStruct(message);

    // Create the request with timeout
    RequestMessageRequest request = RequestMessageRequest.newBuilder()
      .setAddress("hello")
      .setBody(messageBody)
      .setTimeout(Duration.newBuilder().setSeconds(5).build())  // 5 seconds timeout
      .build();

    // Send the request
    grpcClient.request(request).onComplete(ar -> {
      if (ar.succeeded()) {
        EventMessage response = ar.result();
        // Convert Protobuf Struct to JsonObject
        JsonObject responseBody = structToJson(response.getBody());
        System.out.println("Received response: " + responseBody);
      } else {
        System.err.println("Request failed: " + ar.cause());
      }
    });
  }

  public void publishMessage(EventBusBridgeGrpcClient grpcClient) {
    // Create a message
    JsonObject message = new JsonObject().put("value", "Broadcast message");

    // Convert to Protobuf Struct
    Struct messageBody = jsonToStruct(message);

    // Create the request
    PublishMessageRequest request = PublishMessageRequest.newBuilder()
      .setAddress("news")
      .setBody(messageBody)
      .build();

    // Publish the message
    grpcClient.publish(request).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("Message published successfully");
      } else {
        System.err.println("Failed to publish message: " + ar.cause());
      }
    });
  }

  public void subscribeToMessages(EventBusBridgeGrpcClient grpcClient) {
    // Create the subscription request
    SubscribeMessageRequest request = SubscribeMessageRequest.newBuilder()
      .setAddress("news")
      .build();

    // Subscribe to the address
    grpcClient.subscribe(request).onComplete(ar -> {
      if (ar.succeeded()) {
        // Get the stream
        ReadStream<EventMessage> stream = ar.result();

        // Set a handler for incoming messages
        stream.handler(message -> {
          // Store the consumer ID for later unsubscribing
          String consumerId = message.getConsumer();

          // Convert Protobuf Struct to JsonObject
          JsonObject messageBody = structToJson(message.getBody());
          System.out.println("Received message: " + messageBody);
        });

        // Handle end of stream
        stream.endHandler(v -> {
          System.out.println("Stream ended");
        });

        // Handle errors
        stream.exceptionHandler(err -> {
          System.err.println("Stream error: " + err.getMessage());
        });
      } else {
        System.err.println("Failed to subscribe: " + ar.cause());
      }
    });
  }

  public void unsubscribeFromMessages(EventBusBridgeGrpcClient grpcClient, String consumerId) {
    // Create the unsubscribe request with the consumer ID received during subscription
    UnsubscribeMessageRequest request = UnsubscribeMessageRequest.newBuilder()
      .setAddress("news")
      .setConsumer(consumerId)  // The consumer ID received in the subscription
      .build();

    // Unsubscribe
    grpcClient.unsubscribe(request).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("Unsubscribed successfully");
      } else {
        System.err.println("Failed to unsubscribe: " + ar.cause());
      }
    });
  }

  public void healthCheck(EventBusBridgeGrpcClient grpcClient) {
    // Send a ping request
    grpcClient.ping(Empty.getDefaultInstance()).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("Bridge is healthy");
      } else {
        System.err.println("Bridge health check failed: " + ar.cause());
      }
    });
  }

  // Helper methods for JSON <-> Struct conversion
  private Struct jsonToStruct(JsonObject json) {
    // This is a placeholder for the actual conversion method
    // In a real implementation, you would convert JsonObject to Protobuf Struct
    return Struct.getDefaultInstance();
  }

  private JsonObject structToJson(Struct struct) {
    // This is a placeholder for the actual conversion method
    // In a real implementation, you would convert Protobuf Struct to JsonObject
    return new JsonObject();
  }
}
