#!/usr/bin/env node

const Client = require('../index');
const commander = require('commander');
const moduleVersion = require('../package.json').version;
const log = require('winston');
const path = require('path');
const run = (config) => {
    console.log(config);
    var client = new Client(config);
    client.runScript()
        .then((version) => {
            // success
            process.exit();
        })
        .catch((err) => {
            log.error(err);
            process.exit(1);
        });
};

commander
    .version(moduleVersion)
    .usage('[options] <config>')
    .option('-v, --verbose', 'show basic logging information')
    .option('--debug', 'show all logging information')
    .option('-d, --directory [directory]', 'directory to look for changeset files')
    .action((dbConfig, options) => {
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
                changesetDirectory: path.resolve(commander.directory),
                verbose: commander.verbose,
                debug: commander.debug
            });
        }
    })
    .parse(process.argv);
