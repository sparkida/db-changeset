"use strict";

const co = require('co');
const Pg = require('pg');
const format = require('util').format;
const Changeset = require('./changeset');

let client, log;
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

module.exports = class Driver extends Changeset {

    constructor(config) {
        super(config);
        client = this;
        log = client.log;
    }

    getVersionSql() {
        return 'SELECT file_id as "fileId", schema as "schemaVersion" FROM schema_version ORDER BY file_id DESC LIMIT 1';
    }

    getChangesetSql(fileId, schema) {
        return format(
            'INSERT INTO schema_version (file_id,schema) VALUES (%d, %d);',
            fileId,
            schema
        );
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

    connect() {
        return new Promise((resolve, reject) => {
            let conf = client.dbConfig;
            var conString = format("postgres://%s:%s@%s:5432/%s", conf.username, conf.password, conf.host, conf.db);
            client.db = new Pg.Client(conString);
            client.execute = (sql, callback) => {
                client.db.query.call(client.db, sql, callback);
            };
            client.db.connect((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

};
