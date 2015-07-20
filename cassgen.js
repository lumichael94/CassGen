var faker = require('faker');
var cass = require('cassandra-driver');
var async = require('async');

var client = new cass.Client({contactPoints: ['127.0.0.1'], keyspace: 'test'});
var query = "";
var number = 100000;

function connect(callback){

	query = 'CREATE TABLE IF NOT EXISTS random (' +
				'username TEXT, ' +
				'email TEXT, ' +
				'phoneNumber TEXT, ' +
				'hash TEXT, ' +
				'address TEXT, ' +
				'PRIMARY KEY (hash)' +
				');';
	client.execute(query, function(err, result){
		if(err){
			console.log(err);
		} else {
			console.log('Created table.');
		}
		callback(err);
	});
}

function randomRow(callback){
		var uname = String(faker.internet.userName());
	
		query = "INSERT INTO random (username, email, phoneNumber, hash, address) VALUES (" +
				"'" + faker.internet.userName()	+"', " +
				"'" + faker.internet.email()	+"', " +
				"'" + faker.phone.phoneNumber()	+"', " +
				"'" + faker.random.uuid()		+"', " +
				"'" + faker.finance.account()	+"'" +
				");";

		client.execute(query, function(err, result){
			if(err){
				console.log(err);
			} else {
				//console.log('successfull addition');
			}
			callback(err);
		});
}

var tasks = [];

tasks.push(function(callback){
	connect(callback);
});

for(var i = 0; i < number; i++){
	tasks.push(function(callback){
		randomRow(callback);
	});
}

async.series(tasks, function(err){
	if(err){
		console.log(err);
	}
	else{
		console.log('finished');
	}
	process.exit();
});
