import mpi.MPI;
import mpi.MPIException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * This class was made to generify MPI data transfer.
 */
public final class MpiUtils {

    /**
     * @param source the sender node or -1 if any
     * @param tag    the tag of the object or -1 if any
     * @return the wanted data or null
     * @throws MPIException if something bad happened during the reception
     */
    @SuppressWarnings("SameParameterValue")
    public static <T extends Serializable> T receiveObject(int source, int tag) {
        byte[] bytes = new byte[2000];

        try {
            MPI.COMM_WORLD.Recv(bytes, 0, 2000, MPI.BYTE, source == -1 ? MPI.ANY_SOURCE : source, tag == -1 ? MPI.ANY_TAG : tag);

            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = new ObjectInputStream(bis);

            //noinspection unchecked
            T t = (T) in.readObject();
            System.out.printf("Received from source=[%d] and tag=[%d] the object=[%s]%n", source, tag, t.toString());
            return t;
        } catch (IOException | ClassNotFoundException ex) {
            System.out.printf("Error happened while receiving from source=[%d] and tag=[%d]%n", source, tag);
            throw new MPIException(ex);
        }
    }

    /**
     * @param object      Any object that implements serializable
     * @param destination The destination worker which will receive the object
     * @param tag         The tag of the object to identify the type of data
     */
    @SuppressWarnings("SameParameterValue")
    public static <T extends Serializable> void sendObject(T object, int destination, int tag) {
        ByteBuffer byteBuff = ByteBuffer.allocateDirect(2000 + MPI.SEND_OVERHEAD);
        MPI.Buffer_attach(byteBuff);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(object);
            byte[] bytes = bos.toByteArray();
            System.out.printf("Sending to destination=[%d] with tag=[%d] the object of length=[%d]: [%s]%n", destination, tag, bytes.length, object.toString());

            MPI.COMM_WORLD.Isend(bytes, 0, bytes.length, MPI.BYTE, destination, tag);
        } catch (IOException ex) {
            System.out.printf("Error happened while sending to destination=[%d] with tag=[%d] the object [%s]%n", destination, tag, object.toString());
            ex.printStackTrace();
        }
    }
}
