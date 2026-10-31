package org.springframework.data.redis.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/** Redis process owned exclusively by an integration test. */
final class LocalRedisFixture implements AutoCloseable {
    private final Path directory;
    private Process process;
    final int port;

    LocalRedisFixture() throws Exception {
        try (ServerSocket reservation = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            port = reservation.getLocalPort();
        }
        directory = Files.createTempDirectory("redistpl-test-");
        Process started = null;
        try {
            started = new ProcessBuilder("redis-server", "--bind", "127.0.0.1",
                    "--port", Integer.toString(port), "--save", "", "--appendonly", "no",
                    "--dir", directory.toString(), "--logfile", directory.resolve("redis.log").toString())
                    .redirectErrorStream(true).start();
            process = started;
            for (int i = 0; i < 100; i++) {
                if (!started.isAlive()) throw new IOException("Local Redis exited before becoming ready");
                try (Socket socket = new Socket("127.0.0.1", port)) {
                    return;
                } catch (IOException notReady) {
                    Thread.sleep(30);
                }
            }
            throw new IOException("Local Redis did not become ready");
        } catch (Exception failure) {
            try {
                if (started != null) stop(started);
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            } finally {
                try {
                    deleteDirectory();
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            stop(process);
        } finally {
            deleteDirectory();
        }
    }

    private static void stop(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(3, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(3, TimeUnit.SECONDS);
        }
    }

    private void deleteDirectory() throws IOException {
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
