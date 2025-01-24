package io.delta.sharing.server;

import java.util.function.IntConsumer;

public interface TestServer {
    public void start(String pidFileName, IntConsumer callback);
}

