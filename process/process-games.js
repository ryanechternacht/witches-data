var parser = require('./parse.js'),
    rulesEngine = require('./rules_engine.js'),
    DocumentClient = require('documentdb').DocumentClient;
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey});



// loadGames();
// clearDb();


function loadGames() { 
    var nextGame = getNextGameFunc();

    var interval = setInterval(function() { 
        nextGame().then(fetchGame)
        .then(processGame)
        .then(uploadGame)
        .then(function(val) { 
            console.log(val);
        })
        .catch(function(err) { 
            console.log(err);
            if(err == "no more games") {
                clearInterval(interval);
            }
        });
    }, 1000);
}

// METHODS
// returns a function that generates promises, each with a game to load
function getNextGameFunc() { 
    var games = [
    // some games throw an error, so I'm taking them out for now
        "4pLeague_S1_D1L1_G1",
        "4pLeague_S1_D1L1_G2",
        // "4pLeague_S1_D1L1_G3",
        "4pLeague_S1_D1L1_G4",
        // "4pLeague_S1_D1L1_G5",
        // "4pLeague_S1_D1L1_G6",
        // "4pLeague_S2_D1L1_G1",
        "4pLeague_S2_D1L1_G2",
        // "4pLeague_S2_D1L1_G3",
        // "4pLeague_S2_D1L1_G4",
        // "4pLeague_S2_D1L1_G5",
        "4pLeague_S2_D1L1_G6",
        "4pLeague_S3_D1L1_G1",
        "4pLeague_S3_D1L1_G2",
        "4pLeague_S3_D1L1_G3",
        "4pLeague_S3_D1L1_G4",
        "4pLeague_S3_D1L1_G5",
        "4pLeague_S3_D1L1_G6",
        "4pLeague_S4_D1L1_G1",
        "4pLeague_S4_D1L1_G2",
        "4pLeague_S4_D1L1_G3",
        "4pLeague_S4_D1L1_G4",
        "4pLeague_S4_D1L1_G5",
        "4pLeague_S4_D1L1_G6"
    ];

    var i = 0;
    
    return function() { 
        return new Promise(function(resolve, reject) {  
            if(i < games.length) { 
                var game = games[i];
                i++;
                resolve(game);
            }
            else { 
                reject("no more games");
            }
        });
    };
}

function fetchGame(gameName) { 
    return new Promise(function(resolve, reject) { 
        var o = {};
        var q = {query: "SELECT * FROM c where c.id = '" + gameName + "'"};
        var collLink = 'dbs/snellman/colls/games';
        client.queryDocuments(collLink, q, o).toArray(function(err, results) {
            if(err) { 
                reject(err);
            } else if(results.length != 1) { 
                reject("incorrect number of results found: " + results.length);
            } else { 
                resolve(results[0]); 
            }
        });
    });
}

function processGame(game) { 
    return new Promise(function(resolve, reject) { 
        var parsedLog = parser.parseLog(game.ledger);

        var engineSetup = rulesEngine.setupEngine(parsedLog, game.ledger);

        var scoreCards = rulesEngine.processCommands(engineSetup, parsedLog, game.ledger);
        var players = _.sortBy(scoreCards, 'total').reverse();

        var gameComplete = rulesEngine.checkGameComplete(parsedLog);

        resolve({
            name: game.id,
            id: game.id,
            factions: players,
            rounds: engineSetup.rounds, 
            fireAndIceBonus: engineSetup.fireAndIceBonus,
            gameComplete: gameComplete
        });
    });
}

function uploadGame(gameData) { 
    var colLink = 'dbs/dev/colls/games';
    var docLink = colLink + '/docs/' + gameData.Id;
    var options = {};

    return new Promise(function(resolve, reject) { 
        client.deleteDocument(docLink, options, function(err, document) { 
            if(err) { 
                if(err.code != '404') { 
                    reject(err);
                }
            }

            client.createDocument(colLink, gameData, function(err, document) { 
                if(err) { 
                    reject(err);
                }
                else { 
                    resolve("created: " + gameData.name);
                }
            });
        });
    });
}

function clearDb() {
    new Promise(function (resolve, reject) { 
        var o = {};
        var q = {query: "SELECT c.id FROM c"};
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q, o).toArray(function(err, results) {
            if(err) { 
                reject("couldn't pull games");
            }

            resolve(results);
        });
    }).then(function(games) { 
        var i = 0; 
        var interval = setInterval(function() { 
            if(i >= games.length) { 
                clearInterval(interval);
                return;
            }

            var game = games[i].id;
            i++;

            var docLink = 'dbs/dev/colls/games/docs/' + game;
            var options = {};
            new Promise(function(resolve, reject) { 
                client.deleteDocument(docLink, options, function(err, document) { 
                    if(err) { 
                        if(err.code == '404') { 
                            reject("didn't find: " + game);
                        }
                        else { 
                            reject(game + ":\n" + err);
                        }
                    } else { 
                        resolve("successfully deleted: " + game);
                    }
                });
            }).then(console.log)
            .catch(console.log);
        }, 1000); // 1s
    });
}
