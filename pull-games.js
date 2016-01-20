var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('./secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    http = require('http');

var defaults = {
    tempFile: 'games.tmp',
    startDate: moment('2013-05-01', 'YYYY-MM-DD')
}

if(argv['?'] || argv.h || _.contains(argv._, 'help')) { 
    console.log('\t-a \t\t start at first game on snellman');
    console.log('\t-d date \t (YYYY-MM-DD) start at date on snellman');
    console.log('\t-r <file> \t resume from <file>, uses default if none set');
    console.log('\t-f file \t store progress in file');
    console.log('\t-h, -?, help \t print help')
    return;
}

var tempFile;
if(argv.f) { 
    if(typeof argv.f == "string") { 
        tempFile = argv.f;
    } else {
        console.log('BAD ARGUMENTS: -f must pass a file');
        return;
    }
    
}
else { 
    tempFile = defaults.tempFile;
}

var date;
if(argv.d) { 
    if(typeof argv.d == "string") { 
        startDate = moment(argv.d, 'YYYY-MM-DD');
    } else {
        console.log('BAD ARGUMENTS: -d muss pass a date');
        return;
    }
}
else {
    date = defaults.startDate;
}

var host = azureInfo.host;
var masterKey = azureInfo.masterKey;


// var url = 'http://terra.snellman.net/app/results/v2/' + date.format('YYYY/MM/DD');
// console.log(url);
var path = '/app/results/v2/' + date.format('YYYY/MM/DD');
console.log(path);

var options = {
    host: 'terra.snellman.net',
    port: 80,
    path: path,
    method: 'GET'
};

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
        doWork(snell);
    });
}).end(); // end() makes it block

function doWork(snell) { 
    var gameList = _.keys(snell.games);
    for(var i = 0; i < gameList.length; i++) { 
        var game = gameList[i];
        console.log(game);
    }
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



