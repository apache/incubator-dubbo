package org.apache.dubbo.rpc.protocol.tri;


import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SingleProtobufSerialization {
    private static final ConcurrentHashMap<Class<?>, Message> instCache = new ConcurrentHashMap<>();
    private static final ExtensionRegistryLite globalRegistry =
            ExtensionRegistryLite.getEmptyRegistry();
    private final ConcurrentMap<Class<?>, SingleMessageMarshaller<?>> marshallers = new ConcurrentHashMap<>();

    @SuppressWarnings("all")
    public static Message defaultInst(Class<?> clz) {
        Message defaultInst = instCache.get(clz);
        if (defaultInst != null) {
            return defaultInst;
        }
        try {
            defaultInst = (Message) clz.getMethod("getDefaultInstance").invoke(null);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Create default protobuf instance failed ", e);
        }
        instCache.put(clz, defaultInst);
        return defaultInst;
    }

    @SuppressWarnings("all")
    public static <T> Parser<T> getParser(Class<T> clz) {
        Message defaultInst = defaultInst(clz);
        return (Parser<T>) defaultInst.getParserForType();
    }

    public Object deserialize(InputStream in, Class<?> clz) throws IOException {
        try {
            return getMarshaller(clz).parse(in);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    public int serialize(Object obj, OutputStream os) throws IOException {
        final MessageLite msg = (MessageLite) obj;
        msg.writeTo(os);
        return msg.getSerializedSize();
    }

    private SingleMessageMarshaller<?> getMarshaller(Class<?> clz) {
        return marshallers.computeIfAbsent(clz, k -> new SingleMessageMarshaller(k));
    }

    private SingleMessageMarshaller<?> getMarshaller(Object obj) {
        return getMarshaller(obj.getClass());
    }

    public static final class SingleMessageMarshaller<T extends MessageLite> {
        private final Parser<T> parser;
        private final T defaultInstance;

        @SuppressWarnings("unchecked")
        SingleMessageMarshaller(Class<T> clz) {
            this.defaultInstance = (T) defaultInst(clz);
            this.parser = (Parser<T>) defaultInstance.getParserForType();
        }

        @SuppressWarnings("unchecked")
        public Class<T> getMessageClass() {
            // Precisely T since protobuf doesn't let messages extend other messages.
            return (Class<T>) defaultInstance.getClass();
        }

        public T getMessagePrototype() {
            return defaultInstance;
        }

        public T parse(InputStream stream) throws InvalidProtocolBufferException {
            return parser.parseFrom(stream, globalRegistry);
        }

        private T parseFrom(CodedInputStream stream) throws InvalidProtocolBufferException {
            T message = parser.parseFrom(stream, globalRegistry);
            try {
                stream.checkLastTagWas(0);
                return message;
            } catch (InvalidProtocolBufferException e) {
                e.setUnfinishedMessage(message);
                throw e;
            }
        }
    }

}
