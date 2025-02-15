package com.forwarder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class Main
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	public static void main(String... args) throws IOException, URISyntaxException
	{
		var fileName = System.getProperty("kubeconfig");
		var bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8));
		var config = KubeConfig.loadKubeConfig(bufferedReader);
		config.setFile(new File(fileName));

		//I am sure it's not thread safe but let's start with it and later fix.
		var apiClient = Config.fromConfig(config);
		var k8sApiHost = new URI(apiClient.getBasePath()).getAuthority();

		var vertx = Vertx.vertx();
		var server = vertx.createNetServer();

		server.connectHandler(socket -> {
			var resolvedHostPromise = Promise.<Triple<String, String, String>>promise();

			socket.handler(first -> {
				socket.pause(); //there can be a lot of data waiting, so lets pause.

				var bytes = first.getBytes();
				var body = new String(bytes);
				var parts = body.split("\r\n\r\n", 2);
				var headersPart = parts[0];
				var headers = parseHeaders(headersPart);

				var possibleHost = headers.get("host");

				if (possibleHost != null)
				{
					var appAndNamespace = possibleHost.split("\\.");

					if (appAndNamespace.length != 2)
					{
						var msg = "host has invalid format, expected 'service.namespace', received %s".formatted(possibleHost);
						LOGGER.warn(msg);
						resolvedHostPromise.fail(msg);
						return;
					}

					var app = appAndNamespace[0];
					var namespace = appAndNamespace[1];
					LOGGER.info("Resolved app {} and namespace {} based on {}", app, namespace, possibleHost);

					resolvedHostPromise.complete(Triple.of(app, namespace, body));
				}
				else
				{
					var errorMsg = "Host header is empty. Received chunk with possible headers %s".formatted(body);
					resolvedHostPromise.fail(errorMsg);
				}
			});

			resolvedHostPromise.future()//
					.onSuccess(resolvedHost -> {
						var connectionManager = new StatefulConnectionDispatcher(vertx, config, k8sApiHost, socket);

						//let's find out a desired pod id
						var serviceName = resolvedHost.getLeft();
						var namespace = resolvedHost.getMiddle();
						var bodyChunk = resolvedHost.getRight();

						var getPodId = vertx.executeBlocking(() -> {
							var api = new CoreV1Api(apiClient);
							var pods = api.listNamespacedPod(namespace).labelSelector("app=" + serviceName).execute();
							//yea yeah, NLP possible, who cares (for now)
							return pods.getItems().getFirst().getMetadata().getName();
						});

						getPodId.compose(podId -> {
									LOGGER.info("Found target pod podId={} service={} namespace={}", podId, serviceName,
											namespace);
									return connectionManager.initUpstreamConnection(namespace, podId, bodyChunk);
								})//
								.onSuccess(v -> {
									//nothing to do, connection from now on managed by dispatcher.
									LOGGER.info("Successful established port-forward connection !");
								})//
								.onFailure(ex -> {
									LOGGER.info("Failed to establish connection, returning 5xx", ex);

									//do you have better idea how to handle this over plain socket ?
									var httpResponse = """
											HTTP/1.1 500 Internal Server Error\r\n
											Content-Type: text/plain\r
											Content-Length: 35\r
											\r
											500 Internal Server Error occurred
											""";
									socket.write(httpResponse)//
											.compose(v -> socket.close());
								});
					})//
					.onFailure(ex -> {
						LOGGER.info("Failed to establish connection, returning 5xx", ex);

						//do you have better idea how to handle this over plain socket ?
						var httpResponse = """
								HTTP/1.1 500 Internal Server Error\r\n
								Content-Type: text/plain\r
								Content-Length: 35\r
								\r
								500 Internal Server Error occurred
								""";
						socket.write(httpResponse)//
								.compose(v -> socket.close());
					});
		});

		server.listen(80, "localhost");
		LOGGER.info("Application is listening !");
	}

	public static Map<String, String> parseHeaders(String headerString)
	{
		final Map<String, String> headers = new LinkedHashMap<>();
		var lines = headerString.split("\\r?\\n"); // Split by newline

		for (String line : lines)
		{
			int colonIndex = line.indexOf(":");
			if (colonIndex > 0)
			{
				String key = line.substring(0, colonIndex).trim();
				String value = line.substring(colonIndex + 1).trim();
				headers.put(key.toLowerCase(), value.toLowerCase());
			}
		}
		return headers;
	}
}
