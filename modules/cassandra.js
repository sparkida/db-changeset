"use strict";

const co = require('co');
const cassandra = require('cassandra-driver');
const format = require('util').format;
const Changeset = require('./changeset');

let client, log;

module.exports = class Driver extends Changeset {

    constructor(config) {
        super(config);
        client = this;
        log = client.log;
    }

    getVersionSql() {
        return 'SELECT file_id as "fileId", schema_version as "schemaVersion", part FROM schema_version LIMIT 1;';
    }

    getChangesetSql(fileId, schema, part = 0) {
        return format(
            'INSERT INTO %s.schema_version (zero, file_id, schema_version, part) VALUES (0, %d, %d, %d);',
            client.dbConfig.keyspace,
            fileId, 
            schema,
            part
        );
    }

    createTable() {
        return co(function*() {
            var versionQuery = 'SELECT release_version FROM system.local';
            var results = yield client.runQuery(versionQuery);
            var cassandraVersion = Array.isArray(results.rows) && results.rows[0].release_version;
            if (!cassandraVersion) {
                throw new Error('Could not determine the version of Cassandra!');
            }
            log.debug('Cassandra version: ' + cassandraVersion);
            var isVersion3 = cassandraVersion.substr(0, 2) == '3.';
            var schemaKeyspace = isVersion3 ? 'system_schema' : 'system';
            var tablesTable = isVersion3 ? 'tables' : 'schema_columnfamilies';
            var tableNameColumn = isVersion3 ? 'table_name' : 'columnfamily_name';
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
                        + 'zero INT, file_id INT, schema_version INT, part INT, PRIMARY KEY (zero, file_id, schema_version, part)'
                        + ') WITH CLUSTERING ORDER BY (file_id DESC, schema_version DESC, part DESC)',
                        client.dbConfig.keyspace);
                log.info('creating the "schema_version" table...');
                return yield client.runQuery(createQuery);
            }
        });
    }

    connect() {
        return new Promise((resolve, reject) => {
            client.db = new cassandra.Client(client.dbConfig);
            client.execute = (sql, callback) => {
                client.db.execute.call(client.db, sql, callback);
            };
            client.db.on('log', (level, className, message) => {
                if (level === 'info' && client.config.verbose) {
                    log.info(message);
                } else if (level !== 'info' && client.config.debug) {
                    log.debug(message);
                }
            });
            client.db.connect((err) => {
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
    }
};
