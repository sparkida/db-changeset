#!/usr/bin/env node

const Client = require('../index');
const commander = require('commander');
const moduleVersion = require('../package.json').version;
const log = require('winston');

commander
    .version(moduleVersion)
    .command('cassandra-changeset [config...]')
    .option('-v, --verbose', 'show basic logging information')
    .option('-d, --debug', 'show all logging information')
    .action((config, options) => {
        console.log(123124, config);
    })
    .parse(process.argv);

console.log(123, commander);
process.exit();
var configFile = commander.args.pop();

if (!configFile) {
    log.error('could not find configuration file');
    commander.outputHelp();
    process.exit(1);
}

var client = new Client({
    configFile: configFile,
    verbose: commander.verbose,
    debug: commander.debug
});

client.runScript()
    .then((version) => {
        // success
        process.exit(0);
    })
    .catch((err) => {
        log.error(err);
        process.exit(1);
    });
