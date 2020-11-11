package org.greenplum.pxf.api.utilities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.springframework.stereotype.Service;

@Service
public class SerializationService {

    private final KryoPool kryoPool;

    public SerializationService() {
        // A simple factory that creates kryo objects
        KryoFactory factory = Kryo::new;
        kryoPool = new KryoPool.Builder(factory).softReferences().build();
    }

    /**
     * By default, kryo pool uses ConcurrentLinkedQueue which is unbounded. To facilitate reuse of
     * kryo object call releaseKryo() after done using the kryo instance. The class loader for the
     * kryo instance will be set to current thread's context class loader.
     *
     * @return kryo instance
     */
    public Kryo borrowKryo() {
        Kryo kryo = kryoPool.borrow();
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        return kryo;
    }

    /**
     * Release kryo instance back to the pool.
     *
     * @param kryo - kryo instance to be released
     */
    public void releaseKryo(Kryo kryo) {
        kryoPool.release(kryo);
    }
}
