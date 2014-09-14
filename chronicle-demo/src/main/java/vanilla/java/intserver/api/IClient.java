package vanilla.java.intserver.api;

public interface IClient {
    public void response(int request, int response, Object... args);
}
