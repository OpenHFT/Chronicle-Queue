package vanilla.java.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

public class PrintAll implements InvocationHandler {
    private final Object chained;

    PrintAll(Object chained) {
        this.chained = chained;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }
        System.out.println(method.getName() + (args == null ? "()" : Arrays.toString(args)));
        if (chained != null)
            return method.invoke(chained, args);
        return null;
    }

    public static <T> T of(Class<T> interfaceType) {
        return of(interfaceType, null);
    }

    public static <T> T of(Class<T> interfaceType, T t) {
        return (T) Proxy.newProxyInstance(interfaceType.getClassLoader(), new Class[]{interfaceType}, new PrintAll(t));
    }
}
