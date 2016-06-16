#!/usr/bin/env node

const Client = require('../index');
const commander = require('commander');
const moduleVersion = require('../package.json').version;
const winston = require('winston');
const log = new winston.Logger({
    transports: [new winston.transports.Console({colorize: true})]
});
const path = require('path');
const run = (config) => {
    var client = new Client(config);
    client.runScript()
        .then((version) => {
            log.info('success');
            // success
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
    .option('-v, --verbose', 'show basic logging information')
    .option('--debug', 'show all logging information')
    .option('-d, --directory [directory]', 'directory to look for changeset files')
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

