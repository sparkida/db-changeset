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
const getClient = (config) => {
    let Driver = require(format('../modules/%s', config.dbConfig.dialect));
    return new Driver(config);
};
const run = (config) => {
    let client = getClient(config);
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

module.exports.run = run;
module.exports.getClient = getClient;

if (require.main === module) {
    var ran = false;
    commander
        .version(moduleVersion)
        .usage('[options] <config>\n'
            + '  Changeset file format: <fileId>.<schemaVersion>.<ext>\n'
            + '  In the file 3.1.sql, we have fileId=3, schemaVersion=1, ext=sql')
        .option('-n, --name <name>', 'database instance found in config')
        .option('-d, --directory <directory>', 'directory to look for changeset files')
        .option('-t, --target <targetFile>', 'target a version to inclusively run changesets from. '
                + 'By default, the changeset system will use (currentFileId + 1), so the '
                + 'first run results in (0 + 1) = target(1)')
        .option('-u, --update <updateFile>', 'set the current schema version to that of the '
                + 'target changeset file. ie: "... --update 1.1.js" will set file_id to 1 '
                + 'and the schema to 1 in the schema_version table')
        .option('-v, --verbose', 'show basic logging information')
        .option('--debug', 'show all logging information')
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
                assert(commander.name.length > 0, 'the --name is required');
                assert(!!dbConfig[commander.name], 'the --name should match a property in your configuration file');
                dbConfig = dbConfig[commander.name];
                assert(commander.directory.length > 0, 'the --directory is required');
                dir = path.join(commander.directory, commander.name);
                assert(fs.existsSync(dir), '--directory does not exist ' + dir);
                run({
                    dbConfig: dbConfig,
                    name: commander.name,
                    changesetDirectory: dir,
                    targetFile: commander.target,
                    updateFile: commander.update,
                    verbose: commander.verbose,
                    debug: commander.debug
                });
            }
        })
        .parse(process.argv);

    if (!ran) {
        commander.outputHelp();
    }
}
