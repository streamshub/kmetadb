package com.github.eyefloaters.kmetadb;

import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;

import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ApplicationStatus {

    private static Logger log = Logger.getLogger(ApplicationStatus.class);

    @Inject
    ManagedExecutor exec;

    AtomicBoolean running = new AtomicBoolean(true);

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        log.debug("Startup event observed");
        running.set(true);
    }

    void stop(@Observes Shutdown shutdownEvent /* NOSONAR */) {
        log.warn("Shutdown event observed, set running status to false");
        running.set(false);
    }

    public boolean isRunning() {
        return running.get();
    }

    public void assertRunning() {
        if (exec.isShutdown()) {
            log.warn("ManagedExecutor is shutdown, throwing ShutdownException");
            throw new ShutdownException();
        }

        if (!running.get()) {
            log.warn("ApplicationStatus#running is false, throwing ShutdownException");
            throw new ShutdownException();
        }
    }
}
