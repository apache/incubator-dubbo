package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;
import org.apache.dubbo.triple.TripleWrapper;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.dubbo.rpc.protocol.tri.TripleUtil.responseErr;

public class ServerStream extends AbstractStream implements Stream {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);
    private static final String TOO_MANY_REQ = "Too many requests";
    private static final String MISSING_REQ = "Missing request";
    private final Invoker<?> invoker;
    private final MethodDescriptor methodDescriptor;
    private final ChannelHandlerContext ctx;
    private final String serviceName;
    private final String methodName;
    private String serializeType;


    public ServerStream(Invoker<?> invoker, MethodDescriptor methodDescriptor, ChannelHandlerContext ctx, String serviceName, String methodName) {
        super(ctx, TripleUtil.needWrapper(methodDescriptor.getParameterClasses()));
        this.invoker = invoker;
        this.methodDescriptor = methodDescriptor;
        this.ctx = ctx;
        this.serviceName = serviceName;
        this.methodName = methodName;
    }


    @Override
    public void onError(GrpcStatus status) {
    }

    @Override
    public void write(Object obj, ChannelPromise promise) throws Exception {

    }

    public void halfClose() throws Exception {
        if (getData() == null) {
            responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.INTERNAL)
                    .withDescription(MISSING_REQ));
            return;
        }

        Invocation invocation = buildInvocation(serviceName, methodName, methodDescriptor);

        final Result result = this.invoker.invoke(invocation);
        CompletionStage<Object> future = result.thenApply(Function.identity());

        BiConsumer<Object, Throwable> onComplete = (appResult, t) -> {
            try {
                if (t != null) {
                    if (t instanceof TimeoutException) {
                        responseErr(ctx, GrpcStatus.fromCode(Code.DEADLINE_EXCEEDED).withCause(t));
                    }
                    responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN).withCause(t));
                    return;
                }
                AppResponse response = (AppResponse) appResult;
                if (!response.hasException()) {
                    Http2Headers http2Headers = new DefaultHttp2Headers()
                            .status(OK.codeAsText())
                            .set(HttpHeaderNames.CONTENT_TYPE, TripleConstant.CONTENT_PROTO);
                    ctx.write(new DefaultHttp2HeadersFrame(http2Headers));
                    final Message message;
                    if (isNeedWrap()) {
                        message = TripleUtil.wrapResp(this.serializeType, response.getValue(), methodDescriptor);
                    } else {
                        message = (Message) response.getValue();
                    }
                    final ByteBuf buf = TripleUtil.pack(ctx, message);
                    ctx.write(new DefaultHttp2DataFrame(buf));
                    final Http2Headers trailers = new DefaultHttp2Headers()
                            .setInt(TripleConstant.STATUS_KEY, GrpcStatus.Code.OK.code);
                    ctx.write(new DefaultHttp2HeadersFrame(trailers, true));
                } else {
                    final Throwable exception = response.getException();
                    if (exception instanceof TripleRpcException) {
                        responseErr(ctx, ((TripleRpcException) exception).getStatus());
                    } else {
                        responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN)
                                .withCause(exception));
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Exception processing triple message", e);
                if (e instanceof TripleRpcException) {
                    responseErr(ctx, ((TripleRpcException) e).getStatus());
                } else {
                    responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN).withCause(e));
                }
            }
        };

        future.whenComplete(onComplete);
    }

    private Invocation buildInvocation(String serviceName, String methodName, MethodDescriptor methodDescriptor) throws ClassNotFoundException {
        RpcInvocation inv = new RpcInvocation();
        try {
            if (isNeedWrap()) {
                final TripleWrapper.TripleRequestWrapper req = TripleUtil.unpack(getData(), TripleWrapper.TripleRequestWrapper.class);
                this.serializeType = req.getSerializeType();
                final Object[] arguments = TripleUtil.unwrapReq(req, methodDescriptor);
                inv.setArguments(arguments);
            } else {
                final Object req = TripleUtil.unpack(getData(), methodDescriptor.getParameterClasses()[0]);
                inv.setArguments(new Object[]{req});
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize request on server side", e);
        }
        inv.setMethodName(methodName);
        inv.setServiceName(serviceName);
        inv.setTargetServiceUniqueName(serviceName);
        inv.setParameterTypes(methodDescriptor.getParameterClasses());
        inv.setReturnTypes(methodDescriptor.getReturnTypes());
        return inv;
    }
}