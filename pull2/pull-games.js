'use strict';

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    http = require('http'),
    Promise = require('promise'),
    fs = require('fs'),
    path = require('path');


var gameFile = argv['f'],
    logFile = argv['l'] || 'run.tmp',
    deleteFlag = argv['d'],
    timeBetweenPulls = 30000, //30s
    timeBetweenDeletes = 1000, //2s
    host = azureInfo.host,
    masterKey = azureInfo.masterKey; //30s

var client = new DocumentClient(host, {masterKey: masterKey});

// var q = "SELECT * from c where c.id = '4pLeague_S1_D2L2_G1'";
// var collLink = 'dbs/snellman/colls/games';
// client.queryDocuments(collLink, q).toArray(function(err, results) { 
//     if(err) { 
//         console.log(err);
//     } else { 
//        console.log(results);
//     }
// });

// var docLink = 'dbs/snellman/colls/games/docs/4pLeague_S1_D2L2_G1';

// client.readAttachments(docLink).toArray(function(err, results) {
//     if(err) { console.log("failure attachment"); console.log(err); }
//     else {
//         var attach = results[0];
//         var mediaLink = attach.media;
//         client.readMedia(mediaLink, function(err2, attachment) { 
//             if(err2) { console.log("failure media"); console.log(err2); } 
//             else { 
//                 var a = JSON.parse(attachment);
//                 console.dir(a.ledger.length);
//             }
//         });
//     }
// });


// deleteGame("onion")

// if(deleteFlag) { 
//     deleteFromFile(gameFile);
// } else { 
//     pullFromFile(gameFile, logFile);
// }

//  var p = path.join(__dirname, "run.tmp");
// fs.readFile(p, function(err, data) { 
//     if(err) { reject(err); }
//     else { 
//         var log = JSON.parse(data);
//         console.log(log.success.length);
//         console.log(log.failure.length);

//         // var s = "";
//         // for(var i = 0; i < log.success.length; i++) { 
//         //     s += log.success[i].game + "\n";
//         // }
//         // var ps = path.join(__dirname, "success.tmp");
//         // fs.writeFile(ps, s, function(e, d) { });

//         // var f = "";
//         // for(var i = 0; i < log.failure.length; i++) { 
//         //     f += log.failure[i].game + "\n";
//         // }
//         // var pf = path.join(__dirname, "failure.tmp");
//         // fs.writeFile(pf, f, function(e, d) { });
//     }
// });

pullGame("4pLeague_S10_D2L2_G1")
.then(loadGame)
.then(function(status) {
    console.log("success: " + status.game);
})
.catch(function(status) { 
    console.log("failure: " + status.game + " || " + status.step);
});



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



