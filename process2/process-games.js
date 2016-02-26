'use strict';

var parser = require('./parse.js'),
    rulesEngine = require('./rules_engine.js'),
    DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey}),
    timeBetweenParses = 5000, //5s
    timeBetweenDeletes = 2000, //2s
    logFile = argv['l'] || 'run.tmp';



pullGameList()
.then(function(x) { return scheduleParses(x, logFile); })
.then(console.log)
.catch(console.dir);



// pullGame({id: "4pLeague_S10_D3L3_G6"})
// .then(x => parseGame(x, "4pLeague_S10_D3L3_G6"))
// .then(uploadGame)
// .then(console.log)
// .catch(console.dir);

// deleteGame({id: "4pLeague_S10_D3L3_G6"})
// .then(console.log)
// .catch(console.dir);

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
                .then(console.log)
                .catch(console.dir);
            }, timeout, game);
        }
        setTimeout(() => resolve("success"), i * timeBetweenParses);
    });
}

function pullGame(gameName) { 
    return new Promise(function(resolve, reject) { 
        var docLink = 'dbs/snellman/colls/games/docs/' + gameName;

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
            gameComplete: gameComplete
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

function scheduleDeletes(gameList) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < gameList.length; i++) { 
            let game = gameList[i];
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
        var docLink = 'dbs/dev/colls/games/docs/' + game.id;
        client.deleteDocument(docLink, function(err, doc) { 
            if(err) { reject({ game: game, err: err, success: false }); }
            else { resolve({ game: game, success: true }); }
        });
    });
}



