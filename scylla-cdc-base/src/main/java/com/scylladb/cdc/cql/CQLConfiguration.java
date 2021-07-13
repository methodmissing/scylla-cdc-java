package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CQLConfiguration {
    private static final int DEFAULT_PORT = 9042;
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    /**
     * The consistency level of read queries to Scylla.
     */
    public enum ConsistencyLevel {
        /**
         * Waits for a response from single replica
         * in the local data center.
         */
        LOCAL_ONE,
        /**
         * Waits for a response from single replica.
         */
        ONE,
        /**
         * Waits for responses from two replicas.
         */
        TWO,
        /**
         * Waits for responses from three replicas.
         */
        THREE,
        /**
         * Waits for responses from a quorum of replicas
         * in the same DC as the coordinator.
         * <p>
         * Local quorum is defined as:
         * <code>dataCenterReplicationFactor / 2 + 1</code>,
         * where <code>dataCenterReplicationFactor</code> is the
         * configured replication factor for the datacenter of
         * the coordinator node.
         */
        LOCAL_QUORUM,
        /**
         * Waits for responses from a quorum of replicas.
         * <p>
         * Quorum is defined as:
         * <code>(dc1ReplicationFactor + dc2ReplicationFactor + ...) / 2 + 1</code>,
         * where <code>dc1ReplicationFactor</code>, <code>dc2ReplicationFactor</code>, ... are the configured
         * replication factors for all data centers.
         */
        QUORUM,
        /**
         * Waits for responses from all replicas.
         */
        ALL
    }

    public enum AuthMechanism {
        CREDENTIALS,
        CLIENT_CERT
    }

    public final List<InetSocketAddress> contactPoints;
    public final String user;
    public final String password;
    public final String truststoreLocation;
    public final String truststorePassword;
    public final String truststoreType;
    public final String keystoreLocation;
    public final String keystorePassword;
    public final String keystoreType;
    private final ConsistencyLevel consistencyLevel;
    private final String localDCName;
    private final AuthMechanism authMechanism;

    private CQLConfiguration(List<InetSocketAddress> contactPoints,
                            String user, String password, ConsistencyLevel consistencyLevel,
                            String localDCName) {
        this.contactPoints = Preconditions.checkNotNull(contactPoints);
        Preconditions.checkArgument(!contactPoints.isEmpty());

        this.user = user;
        this.password = password;
        this.truststoreLocation = null;
        this.truststorePassword = null;
        this.truststoreType = null;
        this.keystoreLocation = null;
        this.keystorePassword = null;
        this.keystoreType = null;
        // Either someone did not provide credentials
        // or provided user-password pair.
        Preconditions.checkArgument((this.user == null && this.password == null)
                || (this.user != null && this.password != null));

        this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
        this.localDCName = localDCName;
        this.authMechanism = AuthMechanism.CREDENTIALS;
    }

    private CQLConfiguration(List<InetSocketAddress> contactPoints,
                            String truststoreLocation, String truststorePassword, String truststoreType,
                            String keystoreLocation, String keystorePassword, String keystoreType,
                            ConsistencyLevel consistencyLevel, String localDCName) {
        this.contactPoints = Preconditions.checkNotNull(contactPoints);
        Preconditions.checkArgument(!contactPoints.isEmpty());

        this.user = null;
        this.password = null;
        this.truststoreLocation = truststoreLocation;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.keystoreLocation = keystoreLocation;
        this.keystorePassword = keystorePassword;
        this.keystoreType = keystoreType;
        // Either someone did not provide credentials
        // or provided user-password pair.
        Preconditions.checkArgument((this.truststoreLocation == null && this.truststorePassword == null && this.truststoreType == null && this.keystoreLocation == null && this.keystorePassword == null && this.keystoreType == null)
                || (this.truststoreLocation != null && this.truststorePassword != null && this.truststoreType != null && this.keystoreLocation != null && this.keystorePassword != null && this.keystoreType != null));

        this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
        this.localDCName = localDCName;
        this.authMechanism = AuthMechanism.CLIENT_CERT;
    }

    /**
     * Returns the configured consistency level.
     * <p>
     * This consistency level is used in read queries to the
     * CDC log table. The queries to system tables, such
     * as <code>system_distributed.cdc_streams_descriptions_v2</code> do
     * not respect this configuration option.
     *
     * @return configured consistency level.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Returns the name of the configured local datacenter.
     * <p>
     * This local datacenter name will be used to setup
     * the connection to Scylla to prioritize sending requests to
     * the nodes in the local datacenter. If this parameter
     * was not configured, this method returns <code>null</code>.
     *
     * @return the name of configured local datacenter or
     *         <code>null</code> if it was not configured.
     */
    public String getLocalDCName() {
        return localDCName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<InetSocketAddress> contactPoints = new ArrayList<>();
        private String user = null;
        private String password = null;
        private String truststoreLocation = null;
        private String truststorePassword = null;
        private String truststoreType = null;
        private String keystoreLocation = null;
        private String keystorePassword = null;
        private String keystoreType = null;
        private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
        private String localDCName = null;
        private AuthMechanism authMechanism = null;

        public Builder addContactPoint(InetSocketAddress contactPoint) {
            Preconditions.checkNotNull(contactPoint);
            contactPoints.add(contactPoint);
            return this;
        }

        public Builder addContactPoints(Collection<InetSocketAddress> addedContactPoints) {
            for (InetSocketAddress contactPoint : addedContactPoints) {
                this.addContactPoint(contactPoint);
            }
            return this;
        }

        public Builder addContactPoint(String host, int port) {
            Preconditions.checkNotNull(host);
            Preconditions.checkArgument(port > 0 && port < 65536);
            return addContactPoint(new InetSocketAddress(host, port));
        }

        public Builder addContactPoint(String host) {
            return addContactPoint(host, DEFAULT_PORT);
        }

        public Builder withCredentials(String user, String password) {
            this.user = Preconditions.checkNotNull(user);
            this.password = Preconditions.checkNotNull(password);
            this.authMechanism = AuthMechanism.CREDENTIALS;
            return this;
        }

        public Builder withClientCertAuth(String truststoreLocation, String truststorePassword, String truststoreType, String keystoreLocation, String keystorePassword, String keystoreType) {
            this.truststoreLocation = Preconditions.checkNotNull(truststoreLocation);
            this.truststorePassword = Preconditions.checkNotNull(truststorePassword);
            this.truststoreType = Preconditions.checkNotNull(truststoreType);
            this.keystoreLocation = Preconditions.checkNotNull(keystoreLocation);
            this.keystorePassword = Preconditions.checkNotNull(keystorePassword);
            this.keystoreType = Preconditions.checkNotNull(keystoreType);

            this.authMechanism = AuthMechanism.CLIENT_CERT;
            return this;
        }

        /**
         * Sets the consistency level of CDC table read queries.
         * <p>
         * This consistency level is used only for read queries
         * to the CDC log table. The queries to system tables, such
         * as <code>system_distributed.cdc_streams_descriptions_v2</code> do
         * not respect this configuration option.
         *
         * @param consistencyLevel consistency level to set.
         * @return a reference to this builder.
         */
        public Builder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
            return this;
        }

        /**
         * Sets the name of local datacenter.
         * <p>
         * This local datacenter name will be used to setup
         * the connection to Scylla to prioritize sending requests to
         * the nodes in the local datacenter.
         *
         * @param localDCName the name of local datacenter to set.
         * @return a reference to this builder.
         */
        public Builder withLocalDCName(String localDCName) {
            this.localDCName = Preconditions.checkNotNull(localDCName);
            return this;
        }

        public CQLConfiguration build() {
            if (this.authMechanism == AuthMechanism.CREDENTIALS) {
                return new CQLConfiguration(contactPoints, user, password, consistencyLevel, localDCName);
            } else {
                return new CQLConfiguration(contactPoints, truststoreLocation, truststorePassword, truststoreType, keystoreLocation, keystorePassword, keystoreType, consistencyLevel, localDCName);
            }
        }
    }
}