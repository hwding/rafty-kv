package com.amastigote.raftykv.handler;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.LogEntry;
import com.amastigote.raftykv.util.Storage;
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

    private Storage storage = NodeState.storage();

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        /* FIXME: THIS IS ONLY A TEST UTIL, A TEMP SOLUTION */

        final InputStreamReader reader = new InputStreamReader(httpExchange.getRequestBody());
        final String req = new BufferedReader(reader)
                .lines()
                .collect(Collectors.joining());

        reader.close();

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

            NodeState.appendEntryFromClient(entries);

            httpExchange.sendResponseHeaders(HttpResponseStatus.OK.code(), 0L);
            httpExchange.getResponseBody().close();
        } catch (final IllegalArgumentException | JSONException e) {
            log.error("", e);
            httpExchange.sendResponseHeaders(HttpResponseStatus.BAD_REQUEST.code(), 0L);
            httpExchange.getResponseBody().close();
        }
    }
}
