const express = require('express');

const app = express();

app.get('/about', (req, res) => {
    res.send({
        name: 'MongoTSDemoApp',
        description: 'MongoTS demo app',
        version: '0.0.1',
    })
});

app.use(express.static('static'));

app.listen(8080);
