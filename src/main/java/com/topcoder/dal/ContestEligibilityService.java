package com.topcoder.dal;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import com.topcoder.onlinereview.component.contesteligibility.protos.*;

@GrpcService
public class ContestEligibilityService extends ContestEligibilityServiceGrpc.ContestEligibilityServiceImplBase {
    @Override
    public void create(CreateRequest request, StreamObserver<Empty> responseObserver) {

    }
}
