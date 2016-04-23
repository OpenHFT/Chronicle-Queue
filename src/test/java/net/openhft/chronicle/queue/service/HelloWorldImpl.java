package net.openhft.chronicle.queue.service;

/**
 * Created by peter on 23/04/16.
 */
public class HelloWorldImpl implements HelloWorld {
    private final HelloReplier replier;

    public HelloWorldImpl(HelloReplier replier) {
        this.replier = replier;
    }

    @Override
    public void hello(String name) {
        replier.reply("Hello " + name);
    }
}
