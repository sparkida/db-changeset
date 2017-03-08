"use strict";

const co = require('co');
const fs = require('co-fs');
const reduce = require('co-reduce');
const path = require('path');
const format = require('util').format;
const winston = require('winston');
const consoleLogger = new winston.transports.Console({colorize: true});
const log = new winston.Logger({
    transports: [consoleLogger]
});

let client;

module.exports = class Changeset {

    constructor(config) {
        client = this;
        if (!config || !config.dbConfig) {
            throw new Error('must provide at least db connection configuration');
        }
        if (config.updateFile && config.targetFile) {
            throw new Error('found both target and update parameters, use only one');
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
        client.log = log;
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
                if ((ext !== 'js' && ext.search(/[cs]ql/) === -1) || isNaN(fileId)) {
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

    runQuery(query, version) {
        return new Promise(function(resolve, reject) {
            log.info('running query:', query);
            client.execute(query, function(err, results) {
                if (err) {
                    return reject(new Error(format('Error applying changeset %d: %s', version, err)));
                }
                resolve(results);
            });
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

    getVersions() {
        return co(function*() {
            yield client.createTable();
            log.debug('Fetching version info...');
            var versionQuery = client.getVersionSql();
            var result = yield client.runQuery(versionQuery);
            var fileId = 0;
            var schema = 0;
            //TODO what happens if no rows?
            if (!result.rows || !result.rows.length) {
                return null;
            }
            result = result.rows[0];
            log.debug(`Current Schema: fileId=${fileId}, version=${schema}`);
            return result;
        });
    }

    checkFile() {
        let target = client.config.updateFile || client.config.targetFile;
        let files = client.changesetFiles;
        let found = false;
        for (let item of files) {
            if (item.file === target) {
                found = item;
                break;
            }
        }
        if (!found) {
            throw new Error('could not find file: ' + target);
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

    /** changesetItem: {file: <filepath>, ext: <extension>, version: ...} */
    applyChangeset(changesetItem) {
        var file = changesetItem.file;
        var filepath = changesetItem.filepath;
        var fileId = changesetItem.fileId;
        var schema = changesetItem.schema;
        var ext = changesetItem.ext;
        return co(function*() {
            log.info('Applying changeset:', file);
            //if updateVersion is present, we want to blindly set the changeset version
            //assuming this is the current state of the model
            let queryString = client.getChangesetSql(fileId, schema);
            if (client.config.updateFile) {
                log.info(`Setting Schema Version: fileId=${fileId}, schema=${schema}`);
            } else if (ext === 'js') {
                yield require(filepath)(client);
            } else if (ext === 'sql') {
                let query = yield fs.readFile(filepath, 'utf-8');
                queryString = 'BEGIN;\n' + query + queryString + '\nEND;\n';
            } else if (ext === 'cql') {
                let query = yield fs.readFile(filepath, 'utf-8');
                query = query.replace(/\n/gm,'').split(';').filter(Boolean);
                for (let i = 0; i < query.length; i++) {
                    yield client.runQuery(query[i], fileId, schema);
                    yield client.runQuery(client.getChangesetSql(fileId, schema, i + 1));
                }
                return;
            }
            yield client.runQuery(queryString, fileId, schema);
        });
    }

    createTable() {
        throw new Error('must supply your own "createTable" method');
    }

    getChangesetSql(fileId, schema) {
        throw new Error('must supply your own "getChangesetSql" method');
    }

    getVersionSql() {
        throw new Error('must supply your own "getVersionSql" method');
    }

    connect() {
        throw new Error('must supply your own "connect" method');
    }

    runScript() {
        return co(function*() {
            yield client.connect();
            client.changesetFiles = yield client.getChangesets();
            client.versions = yield client.getVersions();
            if (!client.versions) {
                client.versions = {
                    part: 0,
                    fileId: 0,
                    schema: 0
                };
            }
            //if we provide a target version...
            if (client.config.updateFile) {
                let changeset = client.checkFile();
                log.info(`Updating current schema version: ${client.versions.fileId} -> ${changeset.fileId}`);
                yield client.createTable();
                return yield client.applyChangeset(changeset);
            } else if (client.config.targetFile) {
                client.checkFile();
            }
            return yield client.runChanges();
        });
    }
};
