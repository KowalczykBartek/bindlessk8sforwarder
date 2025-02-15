package com.forwarder;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.util.KubeConfig;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.net.NetSocket;

public class StatefulConnectionDispatcher
{
	private static final AtomicInteger CONNECTION_ID = new AtomicInteger(0);
	private static final Logger LOGGER = LoggerFactory.getLogger(StatefulConnectionDispatcher.class);

	private final Vertx vertx;
	private final KubeConfig config;
	private final String k8sApiHost;
	private final NetSocket socket;
	private final int connectionId;

	//in theory, we don't need thread safe variable, cuz we will be called from the same event loop thread
	//but... I haven't verified that.
	private final AtomicInteger initialEventsCounter = new AtomicInteger(0);

	public StatefulConnectionDispatcher(final Vertx vertx, final KubeConfig config, final String k8sApiHost,
			final NetSocket socket)
	{
		this.vertx = vertx;
		this.config = config;
		this.k8sApiHost = k8sApiHost;
		this.socket = socket;
		this.connectionId = CONNECTION_ID.getAndIncrement();
	}

	public Future<Void> initUpstreamConnection(final String namespace, final String podId, final String bodyChunk)
	{
		var uri = "/api/v1/namespaces/%s/pods/%s/portforward?ports=8080".formatted(namespace, podId);

		final WebSocketConnectOptions options = new WebSocketConnectOptions()//
				.addHeader("authorization", "Bearer " + config.getCredentials().get("token"))//
				.addHeader("Upgrade", "websocket")//
				.addHeader("Sec-WebSocket-Protocol", "v5.channel.k8s.io,v4.channel.k8s.io")//
				.addHeader("User-Agent", "Bindlessk8sforwarder")//
				.setHost(k8sApiHost)//
				.setPort(443)//
				.setSubProtocols(List.of("v5.channel.k8s.io", "v4.channel.k8s.io"))//
				.setURI(uri)//
				.setSsl(true);

		var clientOptions = new WebSocketClientOptions();
		clientOptions.setTrustAll(true);

		final WebSocketClient client = vertx.createWebSocketClient(clientOptions);

		var promise = Promise.<Void>promise();
		client.connect(options)//
				.onComplete(res -> {
					if (res.succeeded())
					{
						WebSocket ws = res.result();

						//consider to run this socket's context explicitly.
						ws.handler(buffer -> {
							//first two chunks/frames (for single port forwarder), are some
							//technical, protocol related messages, we can ignore them for now
							var numberOfFramesReceived = initialEventsCounter.getAndIncrement();
							if (numberOfFramesReceived >= 2)
							{
								var withoutStreamId = buffer.slice(1, buffer.length());
								LOGGER.info("{} Received {} over stream id 0", this, new String(buffer.getBytes()));
								socket.write(withoutStreamId);
							}
							else if (numberOfFramesReceived == 1)
							{
								//we just initialized
								LOGGER.info("{} Forwarding initial chunk {} over stream id 0", this, bodyChunk);
								var buff = Buffer.buffer();
								buff.appendByte((byte) 0); //it's a stream id, always use 0 (1 is std err)
								buff.appendBytes(bodyChunk.getBytes());
								ws.writeFinalBinaryFrame(buff);
							}
						});

						//we just switch socket handler from initial to "only forward bytes".
						socket.handler(data -> {
							LOGGER.info("{} Forwarding {} over stream id 0", this, new String(data.getBytes()));
							var buff = Buffer.buffer();
							buff.appendByte((byte) 0); //it's a stream id, always use 0 (1 is std err)
							buff.appendBytes(data.getBytes());

							ws.writeFinalBinaryFrame(buff);
						});

						ws.endHandler(v -> {
							LOGGER.info("{} Closing downstream connection because upstream WS socket closed.", this);
							socket.close();
						});

						socket.endHandler(v -> {
							LOGGER.info("{} Closing upstream WS connection because downstream socket closed.", this);
							ws.close();
						});

						//before leaving handler, let's resume and read from the socket if anything still waiting
						socket.resume();

						LOGGER.info("{} Connected and initialized !", this);
						promise.complete();
					}
					else
					{
						LOGGER.error("{} Unable to establish connection", this, res.cause());
						promise.fail(res.cause());
					}
				});

		return promise.future();
	}

	@Override
	public String toString()
	{
		return new StringJoiner(", ", StatefulConnectionDispatcher.class.getSimpleName() + "[", "]")//
				.add("connectionId=" + connectionId)//
				.toString();
	}
}
