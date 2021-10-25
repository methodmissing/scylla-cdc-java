package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.scylladb.cdc.cql.CQLConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class Driver3Session implements AutoCloseable {
    private final Cluster driverCluster;
    private final Session driverSession;
    private final ConsistencyLevel consistencyLevel;

    public Driver3Session(CQLConfiguration cqlConfiguration) {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);

        clusterBuilder = clusterBuilder.addContactPointsWithPorts(cqlConfiguration.contactPoints);

        String user = cqlConfiguration.user, password = cqlConfiguration.password;
        if (user != null && password != null) {
            clusterBuilder = clusterBuilder.withCredentials(user, password);
        }
        String truststoreLocation = cqlConfiguration.truststoreLocation, truststorePassword = cqlConfiguration.truststorePassword, truststoreType = cqlConfiguration.truststoreType;
        String keystoreLocation = cqlConfiguration.keystoreLocation, keystorePassword = cqlConfiguration.keystorePassword, keystoreType = cqlConfiguration.keystoreType;
        if (truststoreLocation != null && truststorePassword != null && truststoreType != null &&
                keystoreLocation != null && keystorePassword != null && keystoreType != null) {
            try {
                SSLContext sslContext = createSslCustomContext(truststoreLocation, truststorePassword, truststoreType, keystoreLocation, keystorePassword, keystoreType);
                JdkSSLOptions sslOptions = JdkSSLOptions.builder()
                                            .withSSLContext(sslContext)
                                            .build();
                clusterBuilder = clusterBuilder.withSSL(sslOptions);
                clusterBuilder = clusterBuilder.withPort(9142);
            } catch ( KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | KeyManagementException | UnrecoverableKeyException e ) {
                throw new RuntimeException("Exception creating SSL context for client", e);
            }
        }

        if (cqlConfiguration.getLocalDCName() != null) {
            clusterBuilder = clusterBuilder.withLoadBalancingPolicy(
                    DCAwareRoundRobinPolicy.builder().withLocalDc(cqlConfiguration.getLocalDCName()).build());
        }

        driverCluster = clusterBuilder.build();
        driverSession = driverCluster.connect();

        switch (cqlConfiguration.getConsistencyLevel()) {
            case ONE:
                consistencyLevel = ConsistencyLevel.ONE;
                break;
            case TWO:
                consistencyLevel = ConsistencyLevel.TWO;
                break;
            case THREE:
                consistencyLevel = ConsistencyLevel.THREE;
                break;
            case QUORUM:
                consistencyLevel = ConsistencyLevel.QUORUM;
                break;
            case ALL:
                consistencyLevel = ConsistencyLevel.ALL;
                break;
            case LOCAL_QUORUM:
                consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
                break;
            case LOCAL_ONE:
                consistencyLevel = ConsistencyLevel.LOCAL_ONE;
                break;
            default:
                throw new IllegalStateException("Unsupported consistency level: " + cqlConfiguration.getConsistencyLevel());
        }
    }

    public static SSLContext createSslCustomContext(String truststoreLocation, String truststorePassword, String truststoreType,
                                                    String keystoreLocation, String keystorePassword, String keystoreType)
                                                    throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {
        KeyStore tks = KeyStore.getInstance(truststoreType);
        tks.load(new FileInputStream(truststoreLocation), truststorePassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);

        KeyStore cks = KeyStore.getInstance(keystoreType);
        cks.load(new FileInputStream(keystoreLocation), keystorePassword.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(cks, keystorePassword.toCharArray());

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        return context;
    }

    protected Session getDriverSession() {
        return driverSession;
    }

    protected ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void close() {
        if (driverSession != null) {
            driverSession.close();
        }
        if (driverCluster != null) {
            driverCluster.close();
        }
    }
}
