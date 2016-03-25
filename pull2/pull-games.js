'use strict';

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    http = require('http'),
    Promise = require('promise'),
    fs = require('fs'),
    semaphore = require('semaphore')(1),
    path = require('path');


var gameFile = argv['f'],
    logFile = argv['l'] || 'run.tmp',
    deleteFlag = argv['d'],
    timeBetweenPulls = 30000, //30s
    timeBetweenDeletes = 1000, //2s
    host = azureInfo.host,
    masterKey = azureInfo.masterKey; //30s

if(deleteFlag) { 
    deleteFromFile(gameFile);
} else { 
    pullFromFile(gameFile, logFile);
}

function pullFromFile(gameFile, logFile) {
    setupLogFile(logFile)
    .then(function() { return loadGameList(gameFile); })
    .then(function(load) { return scheduleLoads(load, logFile); })
    .then(console.log)
    .catch(function(err) { 
        console.log("error: " + (err.stack || err.toString()));
    });
}

function loadGameList(file) { 
    return new Promise(function(resolve, reject) { 
        var p = path.join(__dirname, file);

        fs.readFile(p, function(err, data) { 
            if(err) { 
                reject(err);
            } else { 
                resolve(JSON.parse(data));
            }
        });
    });
}

function scheduleLoads(load, logFile) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < load.games.length; i++) { 
            let game = load.games[i];
            var timeout = i * timeBetweenPulls;
            setTimeout(function() { 
                // console.log('start pull for: ' + game);
                pullGame(game)
                .then(loadGame)
                .then(function(game) { return logSuccess(game, logFile); })
                .then(function(status) {
                    console.log("success: " + status.game);
                })
                .catch(function(status) { 
                    logFailure({game: game, issue: status}, logFile);
                    console.log("failure: " + status.game + " || " + status.step);
                });
            }, timeout, game);
        }
        setTimeout(function() { 
            resolve("success");
        }, i * timeBetweenPulls);
    });
}

function pullGame(gameName) { 
    return new Promise(function(resolve, reject) {
        var path = '/app/view-game/?game=' + gameName;
        var options = {
            host: 'terra.snellman.net',
            port: 80,
            path: path,
            method: 'GET'
        };
        try { 
            var request = http.request(options);
            request.on('response', function(response) { 
                var data = '';
                response.on('data', function(chunk) { 
                    data += chunk;
                });
                response.on('end', function () {
                    var obj = JSON.parse(data);
                    resolve({
                        gameData: obj,
                        gameName: gameName
                    });
                });
            }).end();
            request.on('error', function(error) { 
                reject(error);
            });
        } catch(err) { 
            reject(err);
        }
    });
}

function loadGame(game) { 
    return new Promise(function(resolve, reject) { 

        var client = new DocumentClient(host, {masterKey: masterKey});

        var collLink = 'dbs/snellman/colls/games';
        var docLink = collLink + '/docs/' + game.gameName;
        var dbOptions = {};

        var obj = {
            id: game.gameName,
            name: game.gameName
        };

        client.createDocument(collLink, obj, dbOptions, function(error, document) {
            if(error) { 
                reject({step: "doc upload", err: error, game: game.gameName});
            } else {
                var attachment = game.gameData;
                attachment.id = game.gameName;
                client.createAttachmentAndUploadMedia(docLink, attachment, dbOptions, 
                    function(error2, document2) { 
                        if(error2) { 
                            reject({step: "attachment upload", err: error2, game: game.gameName});
                        } else {
                            resolve({game: game.gameName, success: true});
                        }
                    });
            }
        });
    });
}

function deleteFromFile(file) { 
    loadGameList(file)
    .then(scheduleDeletes)
    .then(console.log)
    .catch(function(err) { 
        console.log("error: " + err.toString());
    });
}

function scheduleDeletes(file) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < file.games.length; i++) { 
            let game = file.games[i];
            var timeout = i * timeBetweenDeletes;
            setTimeout(function() { 
                // console.log('start pull for: ' + game);
                deleteGame(game)
                .then(function(status) {
                    console.log("successfully deleted: " + status.game);
                })
                .catch(function(status) { 
                    console.log("failure on delete: " + status.game);
                    console.log(status.err);
                });
            }, timeout, game);
        }
        setTimeout(function() { 
            resolve("success");
        }, i * timeBetweenDeletes);
    });
}

function deleteGame(game) {
    return new Promise(function(resolve, reject) {
        var client = new DocumentClient(host, {masterKey: masterKey});

        var docLink = 'dbs/snellman/colls/games/docs/' + game;
        client.deleteDocument(docLink, function(err, doc) { 
            if(err) { reject({ game: game, err: err, success: false }); }
            else { resolve({ game: game, success: true }); }
        });
    });
}

function setupLogFile(file) { 
    return new Promise(function(resolve, reject) { 
        var p = path.join(__dirname, file);

        var obj = { success: [], failure: [] };
        var s = JSON.stringify(obj);

        fs.writeFile(p, s, function(err, data) { 
            if(err) { reject(err); }
            else { 
                resolve(s);
            }
        });
    });
}

function logSuccess(status, file) { 
    return new Promise(function(resolve, reject) { 
        var p = path.join(__dirname, file);

        fs.readFile(p, function(err, data) { 
            if(err) { reject(err); }
            else { 
                var log = JSON.parse(data);
                log.success.push(status);
                var s = JSON.stringify(log);
                fs.writeFile(p, s, function(err2, data2) { 
                    if(err2) { reject(err2); }
                    else { resolve(status); }
                });
            }
        });
    });
}


function logFailure(status, file) { 
    return new Promise(function(resolve, reject) { 
        var p = path.join(__dirname, file);

        fs.readFile(p, function(err, data) { 
            if(err) { reject(err); }
            else { 
                var log = JSON.parse(data);
                log.failure.push(status);
                var s = JSON.stringify(log);
                fs.writeFile(p, s, function(err2, data2) { 
                    if(err2) { reject(err2); }
                    else { resolve(status); }
                });
            }
        });
    });
}



