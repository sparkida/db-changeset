#!/usr/bin/env node

const commander = require('commander');
const format = require('util').format;
const moduleVersion = require('../package.json').version;
const winston = require('winston');
const log = new winston.Logger({
    transports: [new winston.transports.Console({colorize: true})]
});
const path = require('path');
const run = (config) => {
    var conf = config.dbConfig[config.name];
    const Client = require(format('../dbs/%s', conf.dialect));
    config.dbConfig = conf;
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
    // <flag> === required && [flag] === optional
    .usage('[options] <config>')
    .option('-n, --name <name>', 'database instance found in config')
    .option('-d, --directory <directory>', 'directory to look for changeset files')
    .option('-v, --verbose', 'show basic logging information')
    .option('--debug', 'show all logging information')
    .option('-t, --target [version]', 'target a version to run changesets from')
    .option('-u, --update [updateVersion]', 'update schema_version table to target version')
    .action((dbConfig, options) => {
        ran = true;
        if (!dbConfig) {
            commander.outputHelp();
        } else {
            try {
                dbConfig = require(path.resolve(dbConfig));
            } catch (fileNotFound) {
                return log.error('could not find file:', dbConfig);
            }
            run({
                dbConfig: dbConfig,
                name: commander.name,
                changesetDirectory: commander.directory ? path.resolve(commander.directory) : null,
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
    throw new Error('DB connection file required');
}
