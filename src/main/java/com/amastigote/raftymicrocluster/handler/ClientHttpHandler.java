package com.amastigote.raftymicrocluster.handler;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.Storage;
import com.amastigote.raftymicrocluster.protocol.LogEntry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: hwding
 * @date: 2018/12/19
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "HTTP HANDLER")
public class ClientHttpHandler implements HttpHandler {

    private Storage storage = NodeStatus.storage();

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        /* TODO: do role check and redirect before responding */

        final String req = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody()))
                .lines()
                .collect(Collectors.joining());
        final String httpRequestMethod = httpExchange.getRequestMethod().toUpperCase();

        log.info("incoming client req: [{}] {}", httpRequestMethod, req);

        try {
            if (HttpMethod.GET.name().equals(httpRequestMethod)) {
                final JSONArray keys = new JSONArray(req);
                final Object v = storage.get(keys.get(0));

                final String vStr;
                if (Objects.nonNull(v)) {
                    vStr = new JSONObject(v).toString();
                } else {
                    vStr = "null";
                }

                final int vLen = vStr.length();

                log.info("GET from leader result: {}", vStr);

                httpExchange.sendResponseHeaders(HttpResponseStatus.OK.code(), vLen);
                httpExchange.getResponseBody().write(vStr.getBytes());

                httpExchange.getResponseBody().close();
                return;
            }

            final LogEntry.LogCommandType commandType = LogEntry.LogCommandType.valueOf(httpRequestMethod);

            final JSONObject objects = new JSONObject(req);
            final List<LogEntry> entries = objects
                    .toMap()
                    .entrySet()
                    .parallelStream().map(e -> new LogEntry(e.getKey(), e.getValue(), commandType))
                    .collect(Collectors.toList());

            NodeStatus.appendEntryFromClient(entries);

            httpExchange.sendResponseHeaders(HttpResponseStatus.OK.code(), 0L);
            httpExchange.getResponseBody().close();
        } catch (final IllegalArgumentException | JSONException e) {
            log.error("", e);
            httpExchange.sendResponseHeaders(HttpResponseStatus.BAD_REQUEST.code(), 0L);
            httpExchange.getResponseBody().close();
        }
    }
}
