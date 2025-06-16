open module io.vertx.tests.eventbus.bridge.grpc {
    requires io.vertx.core;
    requires io.vertx.eventbusbridge;
    requires io.vertx.eventbus.bridge.grpc;

    requires com.google.protobuf;
    requires com.google.protobuf.util;
    requires io.vertx.grpc.client;
    requires io.vertx.grpc.common;

    requires junit;
    requires io.vertx.testing.unit;
}
