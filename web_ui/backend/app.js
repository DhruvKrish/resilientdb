const express = require('express')
const app = express()
const port = 3000
const fs = require('fs')

function execute(data) {
	command = './run.sh ' + '../.. ' + data.nodes + ' ' + data.clients + ' ' + data.max_txn_inf + ' ' + data.shards + '  > internal.log &'
	const exec = require('child_process').exec
    
	exec(command, (err, stdout, stderr) => {
		process.stdout.write(stdout)
	})
}


app.get('/api/run', (req, res) => {
	if (fs.existsSync('status.log')) {
		res.send({ 'result': 100, 'status': "ResilientDB is already running.." })
		return;
	}
	data = {}
	data.nodes = req.query.nodes
	data.clients = req.query.clients
	data.max_txn_inf = req.query.max_inf
	data.shards = req.query.shards
	execute(data)
	res.send({ 'result': 200, 'status': 'ResilientDB started successfully' })
})
app.get('/api/status', (req, res) => {
	res.send({ 'result': 200, 'status': status({}) })
})

app.listen(port, () => console.log(`Example app listening on port ${port}!`))

function status(options) {
	if (fs.existsSync('status.log')) {
		return fs.readFileSync('status.log', 'utf8').split((/\n/))
	}
	return []
}