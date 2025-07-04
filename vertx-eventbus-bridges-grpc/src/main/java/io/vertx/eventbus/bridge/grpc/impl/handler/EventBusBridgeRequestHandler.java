package io.vertx.eventbus.bridge.grpc.impl.handler;

import com.google.protobuf.Struct;
import com.google.protobuf.util.Durations;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.eventbus.bridge.grpc.BridgeEvent;
import io.vertx.eventbus.bridge.grpc.impl.EventBusBridgeHandlerBase;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.grpc.common.*;
import io.vertx.grpc.event.v1alpha.EventMessage;
import io.vertx.grpc.event.v1alpha.RequestMessageRequest;
import io.vertx.grpc.server.GrpcServerRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class EventBusBridgeRequestHandler extends EventBusBridgeHandlerBase<RequestMessageRequest, EventMessage> {

  public static final ServiceMethod<RequestMessageRequest, EventMessage> SERVICE_METHOD = ServiceMethod.server(
    ServiceName.create("vertx.event.v1alpha.EventBusBridge"),
    "Request",
    GrpcMessageEncoder.encoder(),
    GrpcMessageDecoder.decoder(RequestMessageRequest.newBuilder()));

  public EventBusBridgeRequestHandler(EventBus bus, BridgeOptions options, Handler<BridgeEvent> bridgeEventHandler, Map<String, Pattern> compiledREs) {
    super(bus, options, bridgeEventHandler, compiledREs);
  }

  @Override
  public void handle(GrpcServerRequest<RequestMessageRequest, EventMessage> request) {
    request.handler(eventRequest -> {
      String address = eventRequest.getAddress();
      if (address.isEmpty()) {
        replyStatus(request, GrpcStatus.INVALID_ARGUMENT, "Invalid address");
        return;
      }

      JsonObject body = protoToJson(eventRequest.getBody());
      JsonObject eventJson = createEvent("send", eventRequest);

      if (!checkMatches(true, address)) {
        replyStatus(request, GrpcStatus.PERMISSION_DENIED);
        return;
      }

      checkCallHook(BridgeEventType.SEND, eventJson,
        () -> {
          DeliveryOptions deliveryOptions = createDeliveryOptions(eventRequest.getHeadersMap());
          long timeout = Durations.toMillis(eventRequest.getTimeout());

          if (timeout > 0) {
            deliveryOptions.setSendTimeout(timeout);
          }

          bus.request(address, body, deliveryOptions)
            .onSuccess(reply -> {
              Map<String, String> responseHeaders = new HashMap<>();
              for (Map.Entry<String, String> entry : reply.headers()) {
                responseHeaders.put(entry.getKey(), entry.getValue());
              }

              Struct replyBody;
              if (reply.body() instanceof JsonObject) {
                replyBody = jsonToProto((JsonObject) reply.body(), Struct.newBuilder());
              } else if (reply.body() instanceof String) {
                replyBody = jsonToProto(new JsonObject().put("value", reply.body()), Struct.newBuilder());
              } else {
                replyBody = jsonToProto(new JsonObject().put("value", String.valueOf(reply.body())), Struct.newBuilder());
              }

              EventMessage response = EventMessage.newBuilder()
                .putAllHeaders(responseHeaders)
                .setBody(replyBody)
                .build();

              request.response().end(response);
            })
            .onFailure(err -> {
              EventMessage response = handleErrorAndCreateResponse(err);
              request.response().end(response);
            });
        },
        () -> replyStatus(request, GrpcStatus.PERMISSION_DENIED));
    });
  }

  @Override
  protected JsonObject createEvent(String type, RequestMessageRequest request) {
    JsonObject event = new JsonObject().put("type", type);

    if (request == null) {
      return event;
    }

    // Add address if present
    if (!request.getAddress().isEmpty()) {
      event.put("address", request.getAddress());
    }

    // Add reply address if present
    if (!request.getReplyAddress().isEmpty()) {
      event.put("replyAddress", request.getReplyAddress());
    }

    // Add headers if present
    if (!request.getHeadersMap().isEmpty()) {
      JsonObject headers = new JsonObject();
      request.getHeadersMap().forEach(headers::put);
      event.put("headers", headers);
    }

    return event;
  }
}
