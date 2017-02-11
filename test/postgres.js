const changeset = require('../bin/changeset');
const dbConfig = require('../config.sample.js');
const assert = require('assert');

let client;
let admin;
let config = {
    debug: false,
    verbose: false,
    dbConfig: dbConfig.postgres,
    name: 'postgres',
    changesetDirectory: 'examples/postgres'
};
let connectionGenerator = function*(done, handler, skipConnect) {
    let db = config.dbConfig.db;
    if (!skipConnect) {
        yield admin.connect().then(handler).catch(handler);
    }
    yield admin.db.query(`drop database if exists ${db}`, handler);
    yield admin.db.query(`create database ${db}`, done);
};

before(() => {
    let c = JSON.parse(JSON.stringify(config));
    c.dbConfig.db = 'postgres';
    admin = changeset.getClient(c);
});

beforeEach((done) => {
    let gen = connectionGenerator(done, (err) => {
        if (err) {
            return done(err);
        }
        gen.next();
    }, !!admin.db);
    gen.next();
});

afterEach((done) => {
    client.db.end(done);
});

after((done) => {
    admin.db.end(done);
});

describe('Postgres', () => {
    it ('should update current schema version to target sql file', (done) => {
        let c = Object.assign({updateFile: '1.1.sql'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.db.query('select file_id,schema from schema_version', (err, result) => {
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
            client.db.query('select file_id,schema from schema_version', (err, result) => {
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
        let c = Object.assign({targetFile: '2.2.js'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.db.query('select file_id,schema from schema_version', (err, result) => {
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
            client.db.query('select file_id,schema from schema_version', (err, result) => {
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
