/* jshint ignore:start */
const changeset = require('../bin/changeset');
const dbConfig = require('../config.sample.js');
const pg = require('pg');
const assert = require('assert');

//-n postgres -u 1.1.js --d examples --debug config.sample.js


let client;
let admin;
let pool;
let config = {
    debug: false,
    verbose: false,
    dbConfig: dbConfig.postgres,
    name: 'postgres',
    changesetDirectory: 'examples/postgres'
};
let connectionGenerator = function*(done, handler, skipConnect) {
    let db = dbConfig.postgres.db;
    if (!skipConnect) {
        yield pool.connect(handler);
    }
    yield admin.query(`drop database if exists ${db}`, handler);
    yield admin.query(`create database ${db}`, done);
};

beforeEach((done) => {
    let d = dbConfig.postgres;
    let connectionConfig = {
        user: d.username,
        database: 'postgres',
        host: d.host
    };
    pool = new pg.Pool(connectionConfig);
    let gen = connectionGenerator(done, (err, res) => {
        if (!admin) {
            admin = res;
        }
        gen.next();
    });
    gen.next();
});

afterEach((done) => {
    client.connection.end(done);
});

describe('Postgres', () => {
    it ('should update current schema version to target sql file', (done) => {
        let c = Object.assign({updateFile: '1.1.sql'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.connection.query('select file_id,schema from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 1);
                assert.equal(rows[0].file_id, 1);
                assert.equal(rows[0].schema, 1);
                done();
            });
        }).catch(done);
    });
    it ('should update current schema version to target js file', (done) => {
        let c = Object.assign({updateFile: '2.2.js'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.connection.query('select file_id,schema from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 1);
                assert.equal(rows[0].file_id, 2);
                assert.equal(rows[0].schema, 2);
                done();
            });
        }).catch(done);
    });
    it ('should target a changeset and update schema from that point', (done) => {
        let c = Object.assign({targetFile: '2.2.sql'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.connection.query('select file_id,schema from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 2);
                assert.equal(rows[1].file_id, 3);
                assert.equal(rows[1].schema, 2);
                done();
            });
        }).catch(done);
    });
    it ('should run all changesets', (done) => {
        client = changeset.getClient(config);
        client.runScript().then(() => {
            client.connection.query('select file_id,schema from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 3);
                assert.equal(rows[2].file_id, 3);
                assert.equal(rows[2].schema, 2);
                done();
            });
        }).catch(done);
    });
});
