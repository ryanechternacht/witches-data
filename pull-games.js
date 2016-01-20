var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('./secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    http = require('http'),
    Promise = require('promise'),
    fs = require('fs');

if(argv['?'] || argv.h || _.contains(argv._, 'help')) { 
    console.log('\t-a \t\t start at first game on snellman');
    console.log('\t-d date \t (YYYY-MM-DD) start at date on snellman');
    console.log('\t-r <file> \t resume from <file>, uses default if none set');
    console.log('\t-f file \t store progress in file');
    console.log('\t-h, -?, help \t print help')
    return;
}

var defaults = setupDefaults(),
    tempFile = setupTempFile(argv, defaults),
    date = setupDate(argv, defaults),
    host = azureInfo.host,
    masterKey = azureInfo.masterKey;

lookupDate(date).then(function(snell) { 
    var gameList = _.keys(snell.games);
    writeStatus(tempFile, date, gameList);

    
});


function setupDefaults() {
    return {
        tempFile: 'run.tmp',
        startDate: moment('2013-05-01', 'YYYY-MM-DD')
    };
}

function setupTempFile(argv, defaults) { 
    if(argv.f) { 
        if(typeof argv.f == "string") { 
            return argv.f;
        } else {
            throw new Error('BAD ARGUMENTS: -f must pass a file');
        }
    }
    else { 
        return defaults.tempFile;
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

// returns {player, games}
function lookupDate(date) { 
    var path = '/app/results/v2/' + date.format('YYYY/MM/DD');
    var options = {
        host: 'terra.snellman.net',
        port: 80,
        path: path,
        method: 'GET'
    };

    return new Promise(function (resolve, reject) {
        var request = http.request(options);
        var snell;

        request.on('response', function(response) {
            var data = '';

            response.on('data', function(chunk) {
                // console.log('chunk');
                data += chunk;
            });

            response.on('end', function() { 
                // console.log('end');
                snell = JSON.parse(data);
                resolve({
                    players: snell.players,
                    games: snell.games
                })
            });
        }).end(); // invoke immediately
    });
}

function writeStatus(tempFile, date, gameList) { 
    var data = date.format();
    for(var i = 0; i < gameList.length; i++) { 
        var game = gameList[i];
        data += '\n' + game;
    }

    fs.writeFile(tempFile, data, function(err) {
      if (err) throw err;
      console.log('It\'s saved!');
    });
}

// var today = moment();
// for(/*date*/; date <= today; date = date.add(1, 'day')) { 
//     console.log('http://terra.snellman.net/app/results/v2/' + date.format('YYYY/MM/DD'));
// }

// var client = new DocumentClient(host, {masterKey: masterKey});

// var dQuery = {query: "Select * from c"};
// var dOptions = {};
// var d = '';
// var collLink = 'dbs/snellman-raw/colls/games'
// client.queryDocuments(collLink, dQuery, dOptions).toArray(function(err, results) {
//     if(err) { 
//         console.log("document lookup failed");
//         console.log(err);
//         return;
//     }

//     d = results[0];

//     console.log(d.name);
// });



