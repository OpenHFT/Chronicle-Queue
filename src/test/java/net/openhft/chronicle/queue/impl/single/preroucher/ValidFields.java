package net.openhft.chronicle.queue.impl.single.preroucher;

public enum ValidFields {
    ;

    public static void validateAll(Object... objects) {
        if (objects == null) {
            throw new IllegalStateException("null array");
        }
        for (Object object : objects) {
            if (object instanceof Validatable)
                ((Validatable) object).validate();
        }
    }

}
