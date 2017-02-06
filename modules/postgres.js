"use strict";

const co = require('co');
const fs = require('co-fs');
const Pg = require('pg');
const reduce = require('co-reduce');
const path = require('path');
const format = require('util').format;
const winston = require('winston');
const consoleLogger = new winston.transports.Console({colorize: true});
const log = new winston.Logger({
    transports: [consoleLogger]
});

var client, pg;
class Transaction {

    constructor() {
        this.entries = ['BEGIN;'];
    }

    add(entry) {
        this.entries.push(entry);
    }

    commit() {
        let transaction = this.entries;
        transaction.push('END;');
        return transaction.join('\n');
    }
}

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
        } else if (log.transports.console) {
            //disable logging
            log.remove(consoleLogger);
        }
    }

    beginTransaction() {
        if (client.transaction) {
            throw new Error('Currently only supports single transaction depth');
        }
        //make immutable
        let transaction = new Transaction();
        return {
            add: (entry) => {
                transaction.add(entry);
            },
            commit: (run) => client.runQuery(transaction.commit())
        };
    }

    getChangesets() {
        return co(function*() {
            var dir = path.resolve(client.config.changesetDirectory);
            var exists = yield fs.exists(dir);
            if (!exists) {
                throw new Error('Could not find changeset directory:', dir);
            }
            var files = yield fs.readdir(dir);
            let [targetFileId] = (client.config.targetFile || '0.0').split('.');
            files = yield reduce(files, function*(list, file) {
                let [fileId, schema, ext] = file.split('.');
                if (fileId < targetFileId) {
                    return list;
                }
                if ((ext !== 'js' && ext !== 'sql') || isNaN(fileId)) {
                    return list;
                }
                var filepath = path.join(dir, file);
                var stats = yield fs.stat(filepath);
                if (!stats.isFile()) {
                    return list;
                }
                list.push({
                    file,
                    filepath,
                    ext,
                    fileId: parseInt(fileId),
                    schema: parseInt(schema)
                });
                return list;
            }, []);
            if (files.length < 1) {
                throw new Error('No changeset files found');
            }
            return files;
        });
    }

    createTable() {
        return co(function*() {
            var tableQuery = format(
                "SELECT 1 as found FROM information_schema.tables WHERE table_name='schema_version'"
            );
            var result = yield client.runQuery(tableQuery);
            if (result.rows.length) {
                log.debug('Table "schema_version" already exists.');
            } else {
                var createQuery = 'CREATE TABLE schema_version(file_id SERIAL PRIMARY key, schema INT)';
                log.info('creating the "schema_version" table...');
                return yield client.runQuery(createQuery);
            }
        });
    }

    getVersions() {
        return co(function*() {
            yield client.createTable();
            log.debug('Fetching version info...');
            var versionQuery = 'SELECT file_id,schema FROM schema_version ORDER BY file_id DESC LIMIT 1';
            var result = yield client.runQuery(versionQuery);
            var fileId = 0;
            var schema = 0;
            if (result.rows.length) {
                fileId = parseInt(result.rows[0].file_id);
                schema = parseInt(result.rows[0].schema);
            }
            log.debug(`Current Schema: fileId=${fileId}, version=${schema}`);
            return {fileId, schema};
        });
    }

    runQuery(query, version) {
        return new Promise((resolve, reject) => {
            log.info('running query:', query);
            pg.query(query, (err, results) => {
                if(err) {
                    log.debug(err);
                    return reject(new Error(format('Error applying changeset %s', version)));
                }
                resolve(results);
            });
        });
    }

    applyChangeset(changesetItem) {
        var file = changesetItem.file;
        var filepath = changesetItem.filepath;
        var fileId = changesetItem.fileId;
        var schema = changesetItem.schema;
        var ext = changesetItem.ext;
        var updateChangesetVersion = format(
                'INSERT INTO schema_version (file_id,schema) VALUES (%d, %d);',
                fileId,
                schema
            );
        return co(function*() {
            log.info('Applying changeset:', file);
            //if updateVersion is present, we want to blindly set the changeset version
            //assuming this is the current state of the model
            let queryString = updateChangesetVersion;
            if (client.config.updateFile) {
                log.info(`Setting Schema Version: fileId=${fileId}, schema=${schema}`);
            } else if (ext === 'js') {
                yield require(filepath)(client);
            } else {
                queryString = yield fs.readFile(filepath, 'utf-8');
                queryString = 'BEGIN;\n' + queryString + '\n' 
                    + updateChangesetVersion + '\nEND;\n';
            }
            yield client.runQuery(queryString, fileId, schema);
        });
    }

    runChanges() {
        return client.createTable().then(() => {
            let currentVersion = client.versions.fileId;
            var changesets = client.changesetFiles.filter((item) => {
                return item.fileId > currentVersion;
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
                log.info('No new changesets. Schema version is ' + client.versions.fileId);
                return client.versions.fileId;
            }
            return co(function*() {
                let changeset;
                for (changeset of changesets) {
                    yield client.applyChangeset(changeset);
                }
                log.info('All changesets complete. Schema is now at version', changeset);
            });
        });
    }

    checkUpdate() {
        let updateFile = client.config.updateFile;
        let files = client.changesetFiles;
        let found = false;
        for (let item of files) {
            if (item.file === updateFile) {
                found = item;
                break;
            }
        }
        if (!found) {
            throw new Error('could not find update file: ' + updateFile);
        }
        let targetVersion = found.fileId;
        let currentVersion = client.versions.fileId;
        if (targetVersion === currentVersion) {
            throw new Error('Update target is same as current schema version');
        } else if (targetVersion < currentVersion) {
            throw new Error('Update target is behind current schema version');
        }
        return found;
    }

    runScript() {
        return co(function*() {
            let conf = client.dbConfig;
            var conString = format("postgres://%s:%s@%s:5432/%s", conf.username, conf.password, conf.host, conf.db);
            client.connection = pg = new Pg.Client(conString);
            pg.connect();
            client.changesetFiles = yield client.getChangesets();
            client.versions = yield client.getVersions();
            //if we provide a target version...
            if (client.config.updateFile) {
                let changeset = client.checkUpdate();
                log.info(`Updating current schema version: ${client.versions.fileId} -> ${changeset.fileId}`);
                yield client.createTable();
                return yield client.applyChangeset(changeset);
            }
            return yield client.runChanges();
        });
    }

};
