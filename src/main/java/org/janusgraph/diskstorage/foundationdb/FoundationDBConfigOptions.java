// Copyright 2018 Expero Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the FoundationDB storage backend.
 * These are managed under the 'fdb' namespace in the configuration.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@PreInitializeConfigOptions
public interface FoundationDBConfigOptions {

    ConfigNamespace FDB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "fdb",
        "FoundationDB storage backend options");

    ConfigOption<String> DIRECTORY = new ConfigOption<>(
        FDB_NS,
        "directory",
        "The name of the JanusGraph directory in FoundationDB.  It will be created if it does not exist.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    ConfigOption<String> KEYSPACE = new ConfigOption<>(
            FDB_NS,
            "keyspace",
            "The base path of the JanusGraph directory in FoundationDB.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL,
            "janusgraph");

    ConfigOption<Integer> VERSION = new ConfigOption<>(
        FDB_NS,
        "version",
        "The version of the FoundationDB cluster.",
        ConfigOption.Type.LOCAL,
        620);

    ConfigOption<String> CLUSTER_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "cluster-file-path",
        "Path to the FoundationDB cluster file",
        ConfigOption.Type.LOCAL,
        "default");

    ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
        FDB_NS,
        "isolation-level",
        "Options are serializable, read_committed_no_write, read_committed_with_write",
        ConfigOption.Type.LOCAL,
        "serializable");


    // stevwest: Add support for TLS related configuration settings
    ConfigOption<String> TLS_CERTIFICATE_FILE_PATH = new ConfigOption<>(
            FDB_NS,
            "tls-certificate-file-path",
            "Path to the file containing the TLS certificate for FoundationDB to use.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> TLS_KEY_FILE_PATH = new ConfigOption<>(
            FDB_NS,
            "tls-key-file-path",
            "Path to the file containing the private key corresponding to the TLS certificate for FoundationDB to use.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> TLS_CA_FILE_PATH = new ConfigOption<>(
            FDB_NS,
            "tls-ca-file-path",
            "Path to the file containing the CA certificates chain for FoundationDB to use.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> TLS_VERIFY_PEERS = new ConfigOption<>(
            FDB_NS,
            "tls-verify-peers",
            "The constraints for FoundationDB to validate TLS peers against.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<Integer> FDB_READ_VERSION_FETCH_TIME = new ConfigOption<>(
            FDB_NS,
            "fdb-read-version-fetch-time",
            "The interval of fetching FDB read version.",
            ConfigOption.Type.LOCAL,
            250);

    ConfigOption<Boolean> ENABLE_FDB_READ_VERSION_PREFETCH = new ConfigOption<>(
            FDB_NS,
            "enable-fdb-read-version-prefetch",
            "Whether to prefetch the FDB version or not. Enabling it is to favor performance over consistency.",
            ConfigOption.Type.LOCAL,
            false);

    ConfigOption<Boolean> ENABLE_CAUSAL_READ_RISKY = new ConfigOption<>(
            FDB_NS,
            "enable-causal-read-risky",
            "Enable FDB transaction causal read risky.",
            ConfigOption.Type.LOCAL,
            false
    );

    ConfigOption<Boolean> ENABLE_TRANSACTION_TRACE = new ConfigOption<>(
            FDB_NS,
            "enable-transaction-trace",
            "Enable FDB transaction trace.",
            ConfigOption.Type.LOCAL,
            false
    );

    ConfigOption<String> TRANSACTION_TRACE_PATH = new ConfigOption<>(
            FDB_NS,
            "transaction-trace-path",
            "FDB transaction trace path (a folder, not a file)",
            ConfigOption.Type.LOCAL,
            "/tmp/"
    );

    ConfigOption<String> GET_RANGE_MODE = new ConfigOption<>(
            FDB_NS,
            "get-range-mode",
            "The mod of executing FDB getRange, either `iterator` or `list`",
            ConfigOption.Type.LOCAL,
            "list"
    );

}
