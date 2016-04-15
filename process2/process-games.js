'use strict';

var parser = require('./parse.js'),
    rulesEngine = require('./rules_engine.js'),
    DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    Promise = require('promise'),
    _ = require('underscore'),
    fs = require('fs'),
    path = require('path'),
    semaphore = require('semaphore')(1);

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey}),
    timeBetweenParses = 3000, //5s
    timeBetweenDeletes = 1000, //1s
    logFile = argv['l'] || 'run.tmp',
    deleteFlag = argv['d'],
    gameList = argv['f'];


if(deleteFlag) {
    setupLogFile(logFile)
    .then(x => pullGameList())
    .then(x => scheduleDeletes(x, logFile))
    .then(console.log)
    .catch(console.dir);
}
else {
    setupLogFile(logFile)
    .then(x => pullGameList())
    .then(x => scheduleParses(x, logFile))
    .then(console.log)
    .catch(console.dir);
}

// var p = path.join(__dirname, "run.tmp");
// fs.readFile(p, function(err, data) { 
//     if(err) { reject(err); }
//     else { 
//         var log = JSON.parse(data);
//         console.log(log.success.length);
//         console.log(log.failure.length);

//         var s = "";
//         for(var i = 0; i < log.success.length; i++) { 
//             s += log.success[i].game + "\n";
//         }
//         var ps = path.join(__dirname, "success.tmp");
//         fs.writeFile(ps, s, function(e, d) { });

//         var f = "";
//         for(var i = 0; i < log.failure.length; i++) { 
//             f += log.failure[i].game + "\n";
//         }
//         var pf = path.join(__dirname, "failure.tmp");
//         fs.writeFile(pf, f, function(e, d) { });
//     }
// });

// setupLogFile(logFile)
// .then(x => loadGameList(gameList))
// .then(x => scheduleParses(x, logFile))
// .then(console.log)
// .catch(console.dir);

// var docLink = 'dbs/dev/colls/games/docs/4pLeague_S10_D2L2_G1';
// client.deleteDocument(docLink, function(err, doc) { 
//     if(err) { console.log(err); }
//     else { console.log(doc); }
// });

// pullGame("4pLeague_S10_D1L1_G1")
// .then(x => parseGame(x, "4pLeague_S10_D1L1_G1"))
// // .then(uploadGame)
// // .then(x => console.log("sucess: ", x.game))
// .then(console.log)
// .catch(x => {
//     console.log("failure ", x);
// });

// var q = "select * from c where c.id = '4pLeague_S5_D1L2_G1'";
// var collLink = 'dbs/dev/colls/games';
// client.queryDocuments(collLink, q).toArray(function(err, results) { 
//     if(err) { console.log(err); }
//     else { console.log(results[0]); console.log(results[0]); }
// });


function pullGameList() { 
    return new Promise(function(resolve, reject) { 
        var query = 'select * from c';
        var collLink = 'dbs/snellman/colls/games'
        client.queryDocuments(collLink, query)
            .toArray(function(err, results) { 
                if(err) { reject({ step: "pull game list", err: err }); }
                else { resolve(results); }
        });
    });
}

function loadGameList(file) { 
    return new Promise(function(resolve, reject) { 
        var p = path.join(__dirname, file);

        fs.readFile(p, function(err, data) { 
            if(err) { 
                reject(err);
            } else { 
                var obj = JSON.parse(data);
                var modified = _.map(obj.games, x => ({id:x, name:x}));
                resolve(modified);
            }
        });
    });
}

function scheduleParses(gameList, logFile) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < gameList.length; i++) { 
            let game = gameList[i];
            var timeout = i * timeBetweenParses;
            setTimeout(function(g) { 
                pullGame(game.id)
                .then(x => parseGame(x, game.id))
                .then(uploadGame)
                .then(x => logSuccess(x, logFile))
                .then(x => console.log("sucess: ", x.game))
                .catch(x => {
                    console.log("failure", game);
                    logFailure({game: game.id, issue: x}, logFile);
                });
            }, timeout, game);
        }
        setTimeout(() => resolve("success"), i * timeBetweenParses);
    });
}

function pullGame(gameName) { 
    return new Promise(function(resolve, reject) { 
        var docLink = 'dbs/snellman/colls/games/docs/' + gameName;

        var client = new DocumentClient(host, {masterKey: masterKey});


        client.readAttachments(docLink).toArray(function(err, results) { 
            if(err) { reject({ step: "pull game", err: err, game: gameName }); }
            else { 
                var attach = results[0];
                var mediaLink = attach.media;
                client.readMedia(mediaLink, function(err2, attachment) { 
                    if(err2) { 
                        reject({step: "pull media", err: err2, game: gameName }); 
                    } else { 
                        var a = JSON.parse(attachment);
                        resolve(a);
                    }
                });
            }
        });
    });
}

function parseGame(rawGame, gameName) { 
    return new Promise(function(resolve, reject) { 
        var parsedLog = parser.parseLog(rawGame.ledger);

        var engineSetup = rulesEngine.setupEngine(parsedLog, rawGame.ledger);

        var scoreCards = rulesEngine.processCommands(engineSetup, parsedLog, rawGame.ledger);

        var gameComplete = rulesEngine.checkGameComplete(parsedLog);

        var obj = { 
            id: gameName, 
            name: gameName,
            rounds: engineSetup.rounds,
            fireAndIceBonus: engineSetup.fireAndIceBonus,
            gameComplete: gameComplete,
            bonuses: engineSetup.bonuses,
            results: rulesEngine.buildGameResults(scoreCards)
        };

        obj.factions = _.map(scoreCards, x => x.faction);
        _.each(scoreCards, x => { obj[x.faction] = x; });

        resolve(obj);
    });
}

function uploadGame(gameObj) { 
    return new Promise(function(resolve, reject) { 
        var collLink = 'dbs/dev/colls/games';

        client.createDocument(collLink, gameObj, function(err, document) { 
            if(err) { reject({ step: "upload game", err: err, game: gameObj.id }); }
            else { resolve({ success: true, game: gameObj.id }); }
        });
    });
}

function scheduleDeletes(gameList, logFile) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < gameList.length; i++) { 
            let game = gameList[i];
            var timeout = i * timeBetweenDeletes;
            setTimeout(function() { 
                deleteGame(game)
                .then(x => logSuccess(x, logFile))
                .then(x => console.log("sucess: ", x.game.id))
                .catch(x => {
                    console.log("failure", x);
                    logFailure({game: game, issue: x}, logFile)
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
        var docLink = 'dbs/dev/colls/games/docs/' + game.id;
        client.deleteDocument(docLink, function(err, doc) { 
            if(err) { reject({ game: game, err: err, success: false }); }
            else {  resolve({ game: game, success: true }); }
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
        semaphore.take(() => {
            var p = path.join(__dirname, file);

            fs.readFile(p, function(err, data) { 
                if(err) { 
                    semaphore.leave();
                    reject(err); 
                }
                else { 
                    var log = JSON.parse(data);
                    log.success.push(status);
                    var s = JSON.stringify(log);
                    fs.writeFile(p, s, function(err2, data2) { 
                        if(err2) { 
                            semaphore.leave();
                            reject(err2); 
                        }
                        else { 
                            semaphore.leave();
                            resolve(status); 
                        }
                    });
                }
            });
        });
    });
}


function logFailure(status, file) { 
    return new Promise(function(resolve, reject) { 
        semaphore.take(() => {   
            var p = path.join(__dirname, file);

            fs.readFile(p, function(err, data) { 
                if(err) { 
                    semaphore.leave();
                    reject(err); 
                }
                else { 
                    var log = JSON.parse(data);
                    log.failure.push(status);
                    var s = JSON.stringify(log);
                    fs.writeFile(p, s, function(err2, data2) { 
                        if(err2) { 
                            semaphore.leave();
                            reject(err2); 
                        }
                        else { 
                            semaphore.leave();
                            resolve(status); 
                        }
                    });
                }
            });
        });
    });
}

