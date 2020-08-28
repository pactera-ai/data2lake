const express = require('express');
const { toASCII } = require('punycode');
const app = express()
const port = 7788

app.get('/tables/mysql/:host/:port/:user/:password/:database', (req, res) => {
    var mysql      = require('mysql');
    var connection = mysql.createConnection({
        host     : req.params.host,
        port     : req.params.port,
        user     : req.params.user,
        password : req.params.password,
        database : req.params.database
    });
    let data = [];
    connection.query('show tables', (error, results) => {
        results.forEach( result => {
            let table = {};
            table.schema = req.params.database;
            table.table = JSON.parse(JSON.stringify(result))['Tables_in_'+req.params.database];
            data.push(table);
        })
        res.header('Access-Control-Allow-Origin', '*');
        res.send(data);
    });
})

app.get('/tables/postgres/:host/:port/:user/:password/:database', async (req, res) => {
    var postgres      = require('postgres');
    var connection = postgres({
        host     : req.params.host,
        port     : req.params.port,
        username     : req.params.user,
        password : req.params.password,
        database : req.params.database
    });

    //TODO
    var tables = await connection`show tables`
    tables
    .then( data => {
        console.log(data);
    })
})

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`)
})