"use strict";

const co = require('co');
const fs = require('co-fs');
const reduce = require('co-reduce');
const cassandra = require('cassandra-driver');
const path = require('path');
const log = require('winston');
const format = require('util').format;

var client;

module.exports = class Changeset {

    constructor(config) {
        client = this;
        if (!config || !config.dbConfig) {
            throw new Error('must provide at least db connection configuration');
        }
        client.config = config;
        client.dbConfig = config.dbConfig;
        if (config.debug) {
            log.level = 'debug';
        } else if (config.verbose) {
            log.level = 'info';
        } else {
            //disable logging
            log.remove(log.transports.Console);
        }
    }

    listChangesets() {
        return co(function*() {
            console.log('listing changesets');
            var dir = path.resolve(client.config.changesetDirectory);
            var exists = yield fs.exists(dir);
            if (!exists) {
                throw new Error('Could not find changeset directory:', dir);
            }
            var files = yield fs.readdir(dir);
            files = yield reduce(files, function*(list, file) {
                var parts = file.split('.');
                var version = parseInt(parts[0]);
                var ext = parts[1];
                if ((ext !== 'js' && ext !== 'cql') || isNaN(version)) {
                    return list;
                }
                var filepath = path.join(dir, file);
                var stats = yield fs.stat(filepath);
                if (!stats.isFile()) {
                    return list;
                }
                list.push({
                    file: filepath,
                    ext: ext,
                    version: parseInt(version)
                });
                return list;
            }, []);
            if (files.length < 1) {
                throw new Error('No changeset files found');
            }
            console.log(files);
            return files;
        });
    }

    createVersionTable() {
        return co(function*() {
            var versionQuery = 'SELECT release_version FROM system.local';
            var results = yield client.runQuery(versionQuery);
            var cassandraVersion = results.rows[0] && results.rows[0].release_version;
            if (!cassandraVersion) {
                throw new Error('Could not determine the version of Cassandra!');
            }
            log.debug('Cassandra version: ' + cassandraVersion);
            var isVersion3 = cassandraVersion.substr(0, 2) == '3.';
            var schemaKeyspace = isVersion3 ? 'system_schema' : 'system';
            var tablesTable = isVersion3 ? 'tables' : 'schema_columnfamilies';
            var tableNameColumn = isVersion3 ? 'table_name' : 'columnfamily_name';
            log.debug('schemaKeyspace: ' + schemaKeyspace);
            log.debug('tablesTable: ' + tablesTable);
            log.debug('tableNameColumn: ' + tableNameColumn);
            var tableQuery = format(
                    "SELECT %s FROM %s.%s WHERE keyspace_name='%s'",
                    tableNameColumn, schemaKeyspace, tablesTable, client.dbConfig.keyspace);
            results = yield client.runQuery(tableQuery);
            var tableNames = results.rows.map((row) => {
                return row[tableNameColumn];
            });
            if (tableNames.indexOf('schema_version') > -1) {
                log.debug('Table "schema_version" already exists.');
                return;
            } else {
                var createQuery = format(
                        'CREATE TABLE %s.schema_version ('
                        + 'version INT, date TIMESTAMP, PRIMARY KEY (version))',
                        client.dbConfig.keyspace);
                log.debug('creating the "schema_version" table...');
                return yield client.runQuery(createQuery);
            }
        });
    }

    getSchemaVersion() {
        return co(function*() {
            yield client.createVersionTable();
            log.debug('Fetching version info...');
            var versionQuery = format('SELECT version FROM %s.schema_version LIMIT 1', client.dbConfig.keyspace);
            var results = yield client.runQuery(versionQuery);
            var version = 0;
            if (results.rows.length > 0) {
                if (results.rows[0] && results.rows[0].version) {
                    version = results.rows[0].version;
                }
            }
            version = parseInt(version);
            log.debug('Current version is ' + version);
            return version;
        });
    }

    runQuery(query, version) {
        return new Promise(function(resolve, reject) {
            log.info('running query:', query);
            client.db.execute(query, function(err, results) {
                if (err) {
                    return reject(new Error(format('Error applying changeset %d: %s', version, err)));
                }
                resolve(results);
            });
        });
    }

    /**
     * changesetItem: {file: <filepath>, ext: <extension>, version: ...}
     */
    applyChangeset(changesetItem) {
        var file = changesetItem.file;
        var version = changesetItem.version;
        var ext = changesetItem.ext;
        var updateChangesetVersion = format(
                'INSERT INTO %s.schema_version (version, date) VALUES (%d, %d)',
                client.dbConfig.keyspace, version, Date.now());
        return co(function*() {
            log.info('Applying changeset:', file);
            if (ext === 'js') {
                yield require(file)(client);
                yield client.runQuery(updateChangesetVersion, version);
            } else {
                var queryStrings = yield fs.readFile(file, 'utf-8');
                queryStrings = queryStrings
                    .split('---')
                    .map(function(item) {
                        return item.trim();
                    });
                queryStrings.push(updateChangesetVersion);
                for (var i = 0; i < queryStrings.length; i++) {
                    yield client.runQuery(queryStrings[i], version);
                }
            }
        });
    }

    runChanges() {
        return client.createVersionTable().then(() => {
            var changesets = client.changesetFiles.filter((item) => {
                return item.version > client.schemaVersion;
            }).sort((a, b) => {
                if (a.version > b.version) {
                    return 1;
                }
                if (a.version < b.version) {
                    return -1;
                }
                return 0;
            });
            if (changesets.length < 1) {
                log.info('No new changesets. Schema version is ' + client.schemaVersion);
                return client.schemaVersion;
            }
            var versions = changesets.map(function(item) {
                return item.version;
            });
            versions.unshift(client.schemaVersion);
            log.info('Changing database', versions.join(' -> '));
            return co(function*() {
                for (var i = 0; i < changesets.length; i++) {
                    yield client.applyChangeset(changesets[i]);
                }
                var version = versions.pop();
                log.info('All changesets complete. Schema is now at version', version);
                return version;
            });
        });
    }

    runScript() {
        return co(function*() {
            client.db = new cassandra.Client(client.dbConfig);
            client.db.connect((err, res) => {
                if (err) {
                    log.debug(err);
                    throw new Error('Could not connect to database');
                } else {
                    log.info('cassandra connected');
                }
            });
            client.db.on('log', (level, className, message) => {
                if (level === 'info') {
                    log.debug(message);
                } else {
                    log.warn(message);
                }
            });
            client.changesetFiles = yield client.listChangesets();
            client.schemaVersion = yield client.getSchemaVersion();
            return yield client.runChanges();
        });
    }

};
