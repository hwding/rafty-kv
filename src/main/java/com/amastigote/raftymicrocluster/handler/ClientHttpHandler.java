package com.amastigote.raftymicrocluster.handler;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.LogEntry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: hwding
 * @date: 2018/12/19
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "HTTP HANDLER")
public class ClientHttpHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        String req = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody()))
                .lines()
                .collect(Collectors.joining());
        log.info("incoming client req: {}", req);

        try {
            final LogEntry.LogCommandType commandType = LogEntry.LogCommandType.valueOf(httpExchange.getRequestMethod().toUpperCase());

            if (LogEntry.LogCommandType.PUT.equals(commandType)) {
                JSONObject objects = new JSONObject(req);

                List<LogEntry> entries = objects
                        .toMap()
                        .entrySet()
                        .parallelStream().map(e -> new LogEntry(e.getKey(), e.getValue(), commandType))
                        .collect(Collectors.toList());

                NodeStatus.appendEntryFromClient(entries);

                httpExchange.sendResponseHeaders(HttpResponseStatus.OK.code(), 0L);
                httpExchange.getResponseBody().close();
                return;
            } else if (LogEntry.LogCommandType.GET.equals(commandType)) {

            } else /* LogEntry.LogCommandType.DELETE */ {

            }
        } catch (IllegalArgumentException | JSONException e) {
            httpExchange.sendResponseHeaders(HttpResponseStatus.BAD_REQUEST.code(), 0L);
        }
    }
}
