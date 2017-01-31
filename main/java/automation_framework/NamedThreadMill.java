package automation_framework;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

class NamedThreadMill {

    private String namePrefix = null;
    private boolean daemon = false;
    private int priority = Thread.NORM_PRIORITY;

    public NamedThreadMill setNamePrefix(String namePrefix) {
         if(namePrefix == null){
            throw new NullPointerException();
        }
        this.namePrefix = namePrefix;
        return this;
    }

    public NamedThreadMill setDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadFactory build() {
        return build(this);
    }

    private static ThreadFactory build(NamedThreadMill builder) {

        final String namePrefix = builder.namePrefix;
        final Boolean daemon = builder.daemon;
        final Integer priority = builder.priority;
        final AtomicLong count = new AtomicLong(0);

        return new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                if (namePrefix != null) {
                    thread.setName(namePrefix + "-" + count.getAndIncrement());
                }
                if (daemon != null) {
                    thread.setDaemon(daemon);
                }
                if (priority != null) {
                    thread.setPriority(priority);
                }
                return thread;
            }
        };
    }
}