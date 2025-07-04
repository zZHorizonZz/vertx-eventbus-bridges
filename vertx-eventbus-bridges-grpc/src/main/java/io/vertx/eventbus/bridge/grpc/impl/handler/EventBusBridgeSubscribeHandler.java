package io.vertx.eventbus.bridge.grpc.impl.handler;

import com.google.protobuf.Struct;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.eventbus.bridge.grpc.BridgeEvent;
import io.vertx.eventbus.bridge.grpc.impl.EventBusBridgeHandlerBase;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.grpc.common.*;
import io.vertx.grpc.event.v1alpha.EventMessage;
import io.vertx.grpc.event.v1alpha.SubscribeMessageRequest;
import io.vertx.grpc.server.GrpcServerRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class EventBusBridgeSubscribeHandler extends EventBusBridgeHandlerBase<SubscribeMessageRequest, EventMessage> {

  public static final ServiceMethod<SubscribeMessageRequest, EventMessage> SERVICE_METHOD = ServiceMethod.server(
    ServiceName.create("vertx.event.v1alpha.EventBusBridge"),
    "Subscribe",
    GrpcMessageEncoder.encoder(),
    GrpcMessageDecoder.decoder(SubscribeMessageRequest.newBuilder()));

  public EventBusBridgeSubscribeHandler(EventBus bus, BridgeOptions options, Handler<BridgeEvent> bridgeEventHandler, Map<String, Pattern> compiledREs) {
    super(bus, options, bridgeEventHandler, compiledREs);
  }

  @Override
  public void handle(GrpcServerRequest<SubscribeMessageRequest, EventMessage> request) {
    request.handler(eventRequest -> {
      String address = eventRequest.getAddress();
      if (address.isEmpty()) {
        replyStatus(request, GrpcStatus.INVALID_ARGUMENT, "Invalid address");
        return;
      }

      JsonObject event = createEvent("register", eventRequest);

      if (!checkMatches(false, address)) {
        replyStatus(request, GrpcStatus.PERMISSION_DENIED);
        return;
      }

      checkCallHook(BridgeEventType.REGISTER, event,
        () -> {
          String consumerId = UUID.randomUUID().toString();
          requests.put(consumerId, request);

          MessageConsumer<Object> consumer = bus.consumer(address, new BridgeMessageConsumer(request, address, consumerId));

          Map<String, MessageConsumer<?>> addressConsumers = consumers.computeIfAbsent(address, k -> new ConcurrentHashMap<>());
          addressConsumers.put(consumerId, consumer);

          request.connection().closeHandler(v -> unregisterConsumer(address, consumerId));
        },
        () -> replyStatus(request, GrpcStatus.PERMISSION_DENIED));
    });
  }

  @Override
  protected JsonObject createEvent(String type, SubscribeMessageRequest request) {
    JsonObject event = new JsonObject().put("type", type);

    if (request == null) {
      return event;
    }

    // Add address if present
    if (!request.getAddress().isEmpty()) {
      event.put("address", request.getAddress());
    }

    // Add consumer ID if present
    if (!request.getConsumer().isEmpty()) {
      event.put("consumer", request.getConsumer());
    }

    // Add headers if present
    if (!request.getHeadersMap().isEmpty()) {
      JsonObject headers = new JsonObject();
      request.getHeadersMap().forEach(headers::put);
      event.put("headers", headers);
    }

    return event;
  }

  static final class BridgeMessageConsumer implements Handler<Message<Object>> {
    private final GrpcServerRequest<SubscribeMessageRequest, EventMessage> request;
    private final String address;
    private final String consumerId;

    BridgeMessageConsumer(GrpcServerRequest<SubscribeMessageRequest, EventMessage> request, String address, String consumerId) {
      this.request = request;
      this.address = address;
      this.consumerId = consumerId;
    }

    @Override
    public void handle(Message<Object> message) {
      Map<String, String> responseHeaders = new HashMap<>();
      for (Map.Entry<String, String> entry : message.headers()) {
        responseHeaders.put(entry.getKey(), entry.getValue());
      }

      Struct body;

      if (message.body() instanceof JsonObject) {
        body = jsonToProto((JsonObject) message.body(), Struct.newBuilder());
      } else if (message.body() instanceof String) {
        body = jsonToProto(new JsonObject(String.valueOf(message.body())), Struct.newBuilder());
      } else {
        body = jsonToProto(new JsonObject().put("value", String.valueOf(message.body())), Struct.newBuilder());
      }

      EventMessage response = EventMessage.newBuilder()
        .setAddress(address)
        .setConsumer(consumerId)
        .putAllHeaders(responseHeaders)
        .setBody(body)
        .build();

      if (message.replyAddress() != null) {
        response = response.toBuilder().setReplyAddress(message.replyAddress()).build();
        replies.put(message.replyAddress(), message);
      }

      request.resume();
      request.response().write(response);
      request.pause();
    }
  }
}
