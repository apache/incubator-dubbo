package org.apache.dubbo.rpc.protocol.tri;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AsciiString;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Http2Packet;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.netty4.DubboHttp2ConnectionHandler;
import org.apache.dubbo.remoting.netty4.StreamData;
import org.apache.dubbo.remoting.netty4.StreamHeader;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http2.Http2Error.NO_ERROR;
import static io.netty.util.CharsetUtil.UTF_8;

public class GrpcHttp2FrameListener extends Http2FrameAdapter {
    private TripleProtocol TRIPLE_PROTOCOL = TripleProtocol.getTripleProtocol();
    protected Http2Connection.PropertyKey streamKey = null;
    static final long GRACEFUL_SHUTDOWN_PING = 0x97ACEF001L;

    @Override
    public void onPingAckRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
        System.out.println("on ping ack read data:" + data);
        if (data == GRACEFUL_SHUTDOWN_PING) {
            System.out.println("on ping ack read data shutdown");
        }
    }

    @Override
    public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
        throws Http2Exception {
        System.out.println("onDataRead:" + streamId);
        final DubboHttp2ConnectionHandler connectionHandler = ctx.pipeline().get(DubboHttp2ConnectionHandler.class);
        Http2Connection connection = connectionHandler.encoder().connection();
        Http2Stream stream = connection.stream(streamId);
        Http2Request request = stream == null ? null : (Http2Request) stream.getProperty(streamKey);

        if (request == null || request.getStreamId() != streamId) {
            System.out.println("received remote data from streamId:" + streamId + ", but not found payload.");
            int processed = data.readableBytes() + padding;
            return processed;
        }

        request.cumulate(data);

        int processed = data.readableBytes() + padding;
        if (endOfStream) {
            Invocation invocation = buildInvocation(request.getHeaders(), request.getData());

            Invoker invoker = TRIPLE_PROTOCOL.getInvoker("io.grpc.examples.helloworld.IGreeter");
            Result result = invoker.invoke(invocation);
            CompletionStage<Object> future = result.thenApply(Function.identity());

            future.whenComplete((appResult, t) -> {
                try {
                    if (t == null) {
                        AppResponse response = (AppResponse) appResult;
                        if (!response.hasException()) {
                            Http2Headers http2Headers = new DefaultHttp2Headers()
                                .status(OK.codeAsText())
                                .set(HttpHeaderNames.CONTENT_TYPE, GrpcElf.GRPC_PROTO);
                            StreamHeader streamHeader = new StreamHeader(streamId, http2Headers, false);
                            ctx.channel().write(streamHeader);

                            ByteBuf byteBuf = Marshaller.marshaller.marshaller(ctx.alloc(), response.getValue());
                            StreamData streamData = new StreamData(false, streamId, byteBuf);
                            ctx.channel().write(streamData);
                            //final Http2Headers trailers = new DefaultHttp2Headers()
                            //    .setInt(GrpcElf.GRPC_STATUS, Status.Code.OK.value());
                            //ctx.channel().write(new StreamHeader(streamId, trailers, true));
                        }
                    } else {
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        return processed;
    }

    private Invocation buildInvocation(Http2Headers http2Headers, ByteBuf data) {

        RpcInvocation inv = new RpcInvocation();
        final String path = http2Headers.path().toString();
        String[] parts = path.split("/");
        String serviceName = "io.grpc.examples.helloworld.IGreeter";
        String methodName = "sayHello";
        ServiceRepository repo = ApplicationModel.getServiceRepository();
        MethodDescriptor methodDescriptor = repo.lookupMethod(serviceName, methodName);
        Object obj = Marshaller.marshaller.unmarshaller(methodDescriptor.getParameterClasses()[0], data);
        inv.setMethodName(methodName);
        inv.setServiceName(serviceName);
        inv.setTargetServiceUniqueName(serviceName);
        inv.setParameterTypes(methodDescriptor.getParameterClasses());
        inv.setArguments(new Object[]{obj});
        inv.setReturnTypes(methodDescriptor.getReturnTypes());

        return inv;
    }

    @Override
    public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
        short weight, boolean exclusive, int padding, boolean endStream) throws Http2Exception {
        onHeadersRead(ctx, streamId, headers, padding, endStream);
    }

    @Override
    public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
        boolean endStream) throws Http2Exception {
        System.out.println("onHeadersRead" + streamId);

        final DubboHttp2ConnectionHandler connectionHandler = ctx.pipeline().get(DubboHttp2ConnectionHandler.class);
        if (headers.path() == null) {
            System.out.println("Expected path but is missing");
            return;
        }

        final String path = headers.path().toString();
        if (path.charAt(0) != '/') {
            System.out.println("Expected path but is missing1");
            return;
        }

        final CharSequence contentType = HttpUtil.getMimeType(headers.get(HttpHeaderNames.CONTENT_TYPE));
        if (contentType == null) {
            System.out.println("Expected path but is missing2");
            return;
        }

        if (!GrpcElf.isGrpcContentType(contentType)) {
            System.out.println("Expected path but is missing3");
            return;
        }

        if (!HttpMethod.POST.asciiName().equals(headers.method())) {
            System.out.println("Expected path but is missing4");
            return;
        }

        String marshaller;
        if (AsciiString.contentEquals(contentType, GrpcElf.APPLICATION_GRPC) || AsciiString.contentEquals(contentType, GrpcElf.GRPC_PROTO)) {
            marshaller = "protobuf";
        } else if (AsciiString.contentEquals(contentType, GrpcElf.GRPC_JSON)) {
            marshaller = "protobuf-json";
        } else {
            System.out.println("Expected path but is missing5");
            return;
        }

        Http2Connection connection = connectionHandler.encoder().connection();
        Http2Stream http2Stream = connection.stream(streamId);
        if (streamKey == null) {
            streamKey = connection.newKey();
        }
        Http2Request request = new Http2Request(streamId, http2Stream, headers, streamKey, marshaller,
            ctx.alloc());
        http2Stream.setProperty(streamKey, request);

        if (endStream) {

        }
    }

}