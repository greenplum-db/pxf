package org.greenplum.pxf.service.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.OpensslCipher;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that checks whether native libraries have been loaded and logs
 * information about the loaded native libraries
 */
public class NativeLibraryChecker {

    private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryChecker.class);

    /**
     * Checks whether native libraries are loaded and logs details of the
     * loaded libraries
     */
    public static void checkNativeLibraries() {
        boolean nativeHadoopLoaded = NativeCodeLoader.isNativeCodeLoaded();

        if (nativeHadoopLoaded) {
            Configuration conf = new Configuration();
            boolean zlibLoaded;
            boolean snappyLoaded;
            boolean zStdLoaded;
            boolean bzip2Loaded = Bzip2Factory.isNativeBzip2Loaded(conf);
            boolean openSslLoaded;

            String openSslDetail;
            String hadoopLibraryName;
            String zlibLibraryName = "";
            String snappyLibraryName = "";
            String zstdLibraryName = "";
            String lz4LibraryName;
            String bzip2LibraryName = "";

            hadoopLibraryName = NativeCodeLoader.getLibraryName();
            zlibLoaded = ZlibFactory.isNativeZlibLoaded(conf);
            if (zlibLoaded) {
                zlibLibraryName = ZlibFactory.getLibraryName();
            }
            snappyLoaded = NativeCodeLoader.buildSupportsSnappy() &&
                    SnappyCodec.isNativeCodeLoaded();
            if (snappyLoaded && NativeCodeLoader.buildSupportsSnappy()) {
                snappyLibraryName = SnappyCodec.getLibraryName();
            }
            zStdLoaded = NativeCodeLoader.buildSupportsZstd() &&
                    ZStandardCodec.isNativeCodeLoaded();
            if (zStdLoaded && NativeCodeLoader.buildSupportsZstd()) {
                zstdLibraryName = ZStandardCodec.getLibraryName();
            }
            if (OpensslCipher.getLoadingFailureReason() != null) {
                openSslDetail = OpensslCipher.getLoadingFailureReason();
                openSslLoaded = false;
            } else {
                openSslDetail = OpensslCipher.getLibraryName();
                openSslLoaded = true;
            }
            lz4LibraryName = Lz4Codec.getLibraryName();
            if (bzip2Loaded) {
                bzip2LibraryName = Bzip2Factory.getLibraryName(conf);
            }

            LOG.info("Hadoop native library : {} {}", true, hadoopLibraryName);
            LOG.info("zlib library          : {} {}", zlibLoaded, zlibLibraryName);
            LOG.info("snappy library        : {} {}", snappyLoaded, snappyLibraryName);
            LOG.info("zstd library          : {} {}", zStdLoaded, zstdLibraryName);
            // lz4 is linked within libhadoop
            LOG.info("lz4 library           : {} {}", true, lz4LibraryName);
            LOG.info("bzip2 library         : {} {}", bzip2Loaded, bzip2LibraryName);
            LOG.info("openssl library       : {} {}", openSslLoaded, openSslDetail);
        } else {
            LOG.info("Hadoop native library not loaded");
        }
    }
}
