'use strict';

// TODO a better logging system

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    // http = require('http'),
    // Promise = require('promise'),
    httpSync = require('http-sync'),
    fs = require('fs'),
    sleep = require('sleep'),
    semaphore = require('semaphore')(1),
    path = require('path');

if(argv['?'] || argv.h || _.contains(argv._, 'help')) { 
    console.log('\t-a \t\t start at first game on snellman');
    console.log('\t-d date \t (YYYY-MM-DD) start at date on snellman');
    console.log('\t-r \t\t resume from file set in -f, uses default if none set');
    console.log('\t-s \t\t start a new run even if a temp file exists');
    console.log('\t-f file \t store progress in file');
    console.log('\t-h, -?, help \t print help');
    return;
}


var defaults = setupDefaults(),
    tempFile = setupTempFile(argv, defaults),
    host = azureInfo.host,
    masterKey = azureInfo.masterKey;



pullGames();



function pullGames() { 
    var today = moment();

    // handle resuming or starting a new run
    var date, gameList;
    if(argv['r']) { 
        if(argv['d'] || argv['a']) { 
            console.log("Can't use -d, -a, or -s with -r. Exiting...");
            return;
        }
        if(!fs.existsSync(tempFile)) {
            console.log("Temp file [" + tempFile + "] not found. Exiting...");
            return;
        }

        var load = loadStatusFileSync(tempFile);
        date = moment(load.date);
        gameList = load.gameList;
    } else 
    {
        // unless -s or no tmp file
        if(!(argv['s'] || !fs.existsSync(tempFile))) { 
            console.log('Last run is not finished. Either resume (with -r) or pass -s to start a new run. Exiting...');
            return;
        }
        date = setupDate(argv, defaults);
        var lookup = lookupDateSync(date);
        gameList = lookup.gameList;
        // do something with lookup.players
    }

    var interval = setInterval(function() { 
        console.log('start interval iteration');

        // if last not done, just bail
        if(semaphore.current > 0) { 
            return;
        }

        semaphore.take(function() { 
            // if empty, get new games
            if(gameList.length == 0) { 
                // incrememnt day
                date = date.add(1, 'day');
                console.log(date.format());

                // if we're already up to date, quit
                if(date >= today) { 
                    fs.unlinkSync(tempFile);
                    clearInterval(interval);
                    return;
                }

                var lookup = lookupDateSync(date);
                if(lookup == undefined) { 
                    // try again
                    console.log('date lookup failed');
                    date = date.subtract(1, 'day');
                    semaphore.leave();
                    return;
                };
                gameList = lookup.gameList;
                // do something with lookup.players
            }

            var gameName = gameList.shift();
            var gameData = pullGameSync(gameName);
            if(gameData == undefined) {
                // try again
                console.log('game pull failed');
                gameList.unshift(gameName);
                semaphore.leave();
                return;
            };
            
            console.log('try to upload ' + gameName);
            uploadGame(gameName, gameData, function(err, document) {
                if(err) {
                    console.log('game pull failed');
                    gameList.unshift(gameName);
                } else {
                    writePullGameStatusSync(tempFile, date, gameList);
                }
                semaphore.leave();
            });
        });
    }, 30000); // 30s
}

function setupDefaults() {
    return {
        tempFile: 'run.tmp',
        startDate: moment('2013-05-01', 'YYYY-MM-DD')
    };
}

function setupTempFile(argv, defaults) { 
    if(argv.f) { 
        if(typeof argv.f == "string") { 
            return path.join(__dirname, argv.f);
        } else {
            throw new Error('BAD ARGUMENTS: -f must pass a file');
        }
    }
    else { 
        return path.join(__dirname, defaults.tempFile);
    }
}

function setupDate(argv, defaults) { 
    if(argv.d) { 
        if(typeof argv.d == "string") { 
            return moment(argv.d, 'YYYY-MM-DD');
        } else {
            throw new Error('BAD ARGUMENTS: -d muss pass a date');
        }
    }
    else {
        return defaults.startDate;
    }
}

function loadStatusFileSync(tempFile) { 
    var file = fs.readFileSync(tempFile, 'utf-8'),
        doc = JSON.parse(file);

    return doc;
}

function lookupDateSync(date) { 
    var lookup = pullDateSync(date);

    if(lookup == undefined) { 
        return undefined;
    }

    return {
        gameList: lookup.gameList,
        players: lookup.players
    };
}

// returns {player, games}
function pullDateSync(date) { 
    var path = '/app/results/v2/' + date.format('YYYY/MM/DD');
    var options = {
        host: 'terra.snellman.net',
        port: 80,
        path: path,
        method: 'GET'
    };

    var request = httpSync.request(options);
    var timeout = false;
    request.setTimeout(10000, function() {
        timeout = true;
    });
    var response = request.end(); // execute synchronously
    
    if(timeout) {
        return;
    }
    var data = JSON.parse(response.body.toString());
    return {
        players: data.players,
        games: _.keys(data.games)
    }
}

function writePullGameStatusSync(tempFile, date, gameList) { 
    var doc = {
        date: date,
        gameList: gameList,
        type: 'pullGames'
    };

    fs.writeFileSync(tempFile, JSON.stringify(doc));
}

function pullGameSync(gameName) { 
    var path = '/app/view-game/?game=' + gameName;
    var options = {
        host: 'terra.snellman.net',
        port: 80,
        path: path,
        method: 'GET'
    };

    var request = httpSync.request(options);
    var timeout = false;
    request.setTimeout(10000, function() {
        timeout = true;
    });
    var response = request.end(); // execute synchronously
    
    if(timeout) {
        return undefined;
    }
    var data = JSON.parse(response.body.toString());
    return data;
}

function uploadGame(gameName, gameData, callback) { 
    // manually set id to gameName
    gameData.id = gameName;

    var client = new DocumentClient(host, {masterKey: masterKey});

    var collLink = 'dbs/snellman/colls/games';
    var docLink = collLink + '/docs/' + gameName;
    var dbOptions = {};


    client.deleteDocument(docLink, dbOptions, function(err, document) { 
        if(err) { 
            if(err.code == '404') { 
                // document not found, just keep going
            } else {
                console.log('document deletion failed');
                console.log(err);
            }
        }

        client.createDocument(collLink, gameData, function(err, document) { 
            if(err) { 
                console.log("document addition failed");
                console.log(err);
            }

            // where to log?
            console.log('created document: ' + gameName);

            callback(err, document);
        });
    });
}






