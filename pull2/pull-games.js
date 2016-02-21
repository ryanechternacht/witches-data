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


var file = argv['f'],
    // timeBetweenPulls = 30000; //30s
    timeBetweenPulls = 5000; //30s

loadGameList(file)
.then(scheduleLoads)
.then(console.log)
.catch(function(err) { 
    console.log("error");
    console.log(err);
})



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
    })
}

function scheduleLoads(load) { 
    return new Promise(function(resolve, reject) { 
        var i = 0;
        for(/* i */; i < load.games.length; i++) { 
            var game = load.games[i];
            var timeout = i * timeBetweenPulls;
            setTimeout(loadGame, timeout, game);
        }
        setTimeout(function() { 
            resolve("success");
        }, i * timeBetweenPulls);
    });
}

function loadGame(game) { 
    console.log(game);
}









