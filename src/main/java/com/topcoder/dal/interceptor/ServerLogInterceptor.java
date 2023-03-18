package com.topcoder.dal.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerCall.Listener;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;

@GrpcGlobalServerInterceptor
public class ServerLogInterceptor implements ServerInterceptor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        ServerCall<ReqT, RespT> listener = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {

            @Override
            public void sendMessage(RespT message) {
                logger.info("{} : {}", call.getMethodDescriptor().getFullMethodName(),
                        message.toString().substring(0, Math.min(message.toString().length(), 150)).replaceAll("\n",
                                " "));
                super.sendMessage(message);
            }
        };

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                next.startCall(listener, headers)) {

            @Override
            public void onMessage(ReqT message) {
                logger.info("{} : {}", call.getMethodDescriptor().getFullMethodName(),
                        message.toString().replaceAll("\n", " "));
                super.onMessage(message);
            }
        };
    }
}