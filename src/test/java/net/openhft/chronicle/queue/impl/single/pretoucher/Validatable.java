package net.openhft.chronicle.queue.impl.single.pretoucher;

public interface Validatable {

    /**
     * Method which can be called when writing DTOs via the method writer.
     * <p>
     * This can be generated using ValidField.main {classname}
     *
     * @throws IllegalStateException if a value is out of range.
     */
    default void validate() throws IllegalStateException {

    }
}