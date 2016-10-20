#!/usr/bin/env node

const commander = require('commander');
const format = require('util').format;
const moduleVersion = require('../package.json').version;
const winston = require('winston');
const log = new winston.Logger({
    transports: [new winston.transports.Console({colorize: true})]
});
const assert = require('assert');
const path = require('path');
const fs = require('fs');
const run = (config) => {
    var conf = config.dbConfig;
    const Client = require(format('../modules/%s', conf.dialect));
    var client = new Client(config);
    client.runScript()
        .then((version) => {
            log.info('success');
            process.exit();
        })
        .catch((err) => {
            log.error(err);
            process.exit(1);
        });
};

var ran = false;
commander
    .version(moduleVersion)
    .usage('[options] <config>')
    .option('-n, --name <name>', 'database instance found in config')
    .option('-d, --directory <directory>', 'directory to look for changeset files')
    .option('-v, --verbose', 'show basic logging information')
    .option('--debug', 'show all logging information')
    .option('-t, --target <version>', 'target a version to run changesets from')
    .option('-u, --update <updateVersion>', 'update schema_version table to target version')
    .action((dbConfig, options) => {
        ran = true;
        if (!dbConfig) {
            commander.outputHelp();
        } else {
            let dir;
            try {
                dbConfig = require(path.resolve(dbConfig));
            } catch (fileNotFound) {
                return log.error('could not find file:', dbConfig);
            }
            try {
                assert(commander.name.length > 0, 'the --name is required');
                assert(!!dbConfig[commander.name], 'the --name should match a property in your configuration file');
                dbConfig = dbConfig[commander.name];
                assert(commander.directory.length > 0, 'the --directory is required');
                dir = path.join(commander.directory, commander.name);
                assert(fs.existsSync(dir), '--directory does not exist ' + dir);
            } catch (e) {
                process.stdout.write(e.message + '\n');
                process.exit(1);
            }
            run({
                dbConfig: dbConfig,
                name: commander.name,
                changesetDirectory: dir,
                version: commander.target,
                updateVersion: commander.update,
                verbose: commander.verbose,
                debug: commander.debug
            });
        }
    })
    .parse(process.argv);

if (!ran) {
    commander.outputHelp();
}
